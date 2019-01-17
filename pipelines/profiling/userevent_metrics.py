import logging
from sqlalchemy.exc import IntegrityError
import helper
from datasources import PipelineIO
from datasources.database.database import db
from datasources.database.model import User, Event, UserEvent

logger = logging.getLogger(__name__)


class UserEventMetrics:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['stream', 'nodes', 'event'], stage_input, stage_input_format)
        self.output_prefix = 'uem'
        self.output_format = {
            'userevent_metrics': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'userevent_metrics'),
                'r_kwargs': {
                    'dtype': {
                        # 'user_id': 'uint32',
                        'user_name': str,
                        'topical_attachment': 'float32',
                        'topical_focus': 'float32',
                        'topical_strength': 'float32'
                    }
                },
                'w_kwargs': {'index': False}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.skip_output_check or not self.output:
            stream = self.__add_stream_categories(self.input['stream'], self.input['event'])
            topical_attachment = self.__topical_attachment(stream)
            topical_focus = self.__topical_focus(self.input['stream'])
            topical_strength = self.__topical_strength(self.input['stream'])
            self.output['userevent_metrics'] =\
                self.__merge_metrics(self.input['nodes'], topical_attachment, topical_focus, topical_strength)

            if self.config.save_db_output:
                self.__persist_userevents(self.output['userevent_metrics'], self.config.dataset_name)

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __add_stream_categories(stream, event):
        logger.info('adding tw stream categories for users')

        hashtags = event['hashtags'][0]

        stream['tw_ontopic'] = stream['hashtags'].apply(lambda t: any(h in hashtags for h in t))
        stream['link_ontopic'] = stream[stream['tw_ontopic']]['urls'].apply(lambda t: t != [''])
        stream['link_ontopic'].fillna(False, inplace=True)

        return stream

    @staticmethod
    def __topical_attachment(stream):
        logger.info('compute topical attachment')

        def topical_attachment_alg(tw_ontopic, tw_offtopic, link_ontopic, link_offtopic):
            return (tw_ontopic + link_ontopic) / (tw_offtopic + link_offtopic + 1)

        topical_attachment =\
            stream[['author', 'tw_ontopic', 'link_ontopic']].groupby('author')\
                .apply(lambda x: topical_attachment_alg(x['tw_ontopic'].sum(), (~x['tw_ontopic']).sum(),
                                                        x['link_ontopic'].sum(), (~x['link_ontopic']).sum()))\
                .to_frame().rename(columns={0: 'topical_attachment'})

        logger.debug(helper.df_tostring(topical_attachment, 5))

        return topical_attachment

    @staticmethod
    def __topical_focus(stream):
        logger.info('compute event focus')

        def topical_focus_alg(t_ontopic, t_offtopic):
            return t_ontopic / (t_offtopic + 1)

        topical_focus =\
            stream[['author', 'tw_ontopic']].groupby('author')\
                .apply(lambda x: topical_focus_alg(x['tw_ontopic'].sum(), (~x['tw_ontopic']).sum()))\
                .to_frame().rename(columns={0: 'topical_focus'})

        logger.debug(helper.df_tostring(topical_focus, 5))

        return topical_focus

    @staticmethod
    def __topical_strength(stream):
        logger.info('compute topical strength')

        def topical_strength_alg(link_ontopic, link_offtopic, rtw_ontopic, rtw_offtopic):
            import math
            return (link_ontopic * math.log10(link_ontopic + rtw_ontopic + 1)) / \
                   (link_offtopic * math.log10(link_offtopic + rtw_offtopic + 1) + 1)

        topical_strength =\
            stream[['author', 'tw_ontopic', 'link_ontopic', 'no_retweets']].groupby('author')\
                .apply(lambda x: topical_strength_alg(x['link_ontopic'].sum(), (~x['link_ontopic']).sum(),
                                                      x[x['tw_ontopic']]['no_retweets'].sum(),
                                                      x[~x['tw_ontopic']]['no_retweets'].sum()))\
                .to_frame().rename(columns={0: 'topical_strength'})

        logger.debug(helper.df_tostring(topical_strength, 5))

        return topical_strength

    @staticmethod
    def __merge_metrics(nodes, topical_attachment, topical_focus, topical_strength):
        logger.info('merging userevent metrics')

        userevents = topical_attachment\
            .merge(topical_focus, left_index=True, right_index=True)\
            .merge(topical_strength, left_index=True, right_index=True)\
            .reset_index().rename(columns={'author': 'user_name'})

        # add missing nodes
        userevents = userevents.merge(nodes[['user_name']], left_on='user_name', right_on='user_name',
                                      how='outer', sort='True').fillna(0)

        logger.debug(helper.df_tostring(userevents, 5))

        return userevents

    @staticmethod
    def __persist_userevents(userevents, dataset_name):
        logger.info('persist userevent metrics')
        userevent_records = userevents.set_index('user_name').to_dict('index')

        try:
            with db.session_scope() as session:
                # get all users for current dataset
                user_entities = session.query(User)\
                    .filter(User.user_name.in_(userevent_records.keys())).all()

                # get current event
                event_entity = session.query(Event).filter(Event.name == dataset_name).first()

                userevent_entities = []
                for user_name, metrics in userevent_records.items():
                    # get user entities and userevent info
                    user_entity = next(filter(lambda x: x.user_name == user_name, user_entities), None)

                    # create userevent entity
                    userevent_entity = UserEvent(**metrics, user=user_entity, event=event_entity)
                    userevent_entities.append(userevent_entity)

                session.add_all(userevent_entities)
            logger.debug('userevent info successfully persisted')
        except IntegrityError:
            logger.debug('userevent info already exists or constraint is violated and could not be added')
