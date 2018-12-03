import pandas as pd
import logging
from sqlalchemy.exc import IntegrityError
import helper
from datasources import PipelineIO
from datasources.database.database import session_scope
from datasources.database.model import User, Event, UserEvent
from datasources.tw.helper import query_builder
from datasources.tw.tw import tw

logger = logging.getLogger(__name__)


class UserEventMetrics:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['nodes', 'userinfo'], stage_input, stage_input_format)
        self.output_prefix = 'uem'
        self.output_format = {
            'stream': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'stream'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'author': str,
                        'date': str,
                        'language': str,
                        'text': str,
                        'no_replies': 'uint32',
                        'no_retweets': 'uint32',
                        'no_likes': 'uint32'
                    },
                    'converters': {
                        'reply': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'hashtags': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'emojis': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'urls': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'mentions': lambda x: x.strip('[]').replace('\'', '').split(', ')
                    }
                },
                'w_kwargs': {'index': False}
            },
            'topical_attachment': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'topical_attachment'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'topical_attachment': 'float32'
                    }
                },
                'w_kwargs': {'index': False}
            },
            'event_focus': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'event_focus'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'event_focus': 'float32'
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
            event = self.__get_event(self.config.dataset_name)
            self.output['stream'] = self.__get_stream(self.input['nodes'], event)
            self.output['topical_attachment'] = self.__topical_attachment(self.output['stream'], event)
            self.output['event_focus'] = self.__event_focus(self.output['stream'], event)

            if self.config.save_db_output:
                self.__persist_profile(self.output['topical_attachment'], self.output['event_focus'],
                                       self.config.dataset_name)

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __get_event(dataset_name):
        logger.info('getting event')
        with session_scope() as session:
            event = session.query(Event.name, Event.start_date, Event.end_date, Event.location, Event.hashtags)\
                .filter(Event.name == dataset_name).first()._asdict()

        event['hashtags'] = event['hashtags'].split()

        logger.debug(f'event: {event}')

        return event

    @staticmethod
    def __get_stream(nodes, event):
        logger.info('getting tw stream for users')

        user_names = nodes['user_name'].drop_duplicates().tolist()

        streams = []
        for u in user_names:
            query = query_builder(
                people={'from': u},
                date={
                    'since': event['start_date'].strftime('%Y-%m-%d'),
                    'until': event['end_date'].strftime('%Y-%m-%d')
                })
            streams.extend(tw.tw_dynamic_scraper.search(query))

        df_stream = pd.DataFrame.from_records(streams)

        logger.debug(helper.df_tostring(df_stream, 5))

        return df_stream

    @staticmethod
    def __topical_attachment(stream, event):
        logger.info('compute topical attachment')

        def topical_attachment_alg(t_ontopic, t_offtopic, l_ontopic, l_offtopic):
            return (t_ontopic + l_ontopic) / (t_offtopic + l_offtopic + 1)

        topical_attachments = []
        for u_name, u_stream in stream[['author', 'hashtags', 'urls']].groupby('author'):
            ontopic_mask = u_stream['hashtags'].apply(lambda t: any(h in event['hashtags'] for h in t))

            tw_ontopic = u_stream[ontopic_mask].shape[0]
            tw_offtopic = u_stream.shape[0] - tw_ontopic

            link_ontopic = sum(u_stream[ontopic_mask]['urls'].apply(lambda t: t != ['']))
            link_offtopic = sum(u_stream[~ontopic_mask]['urls'].apply(lambda t: t != ['']))

            topical_attachments.append({
                'user_name': u_name,
                'topical_attachment': topical_attachment_alg(tw_ontopic, tw_offtopic, link_ontopic, link_offtopic)
            })

        df_topicalattachments = pd.DataFrame.from_records(topical_attachments)

        logger.debug(helper.df_tostring(df_topicalattachments, 5))

        return df_topicalattachments

    @staticmethod
    def __event_focus(stream, event):
        logger.info('compute event_focus')

        def event_focus_alg(t_ontopic, t_offtopic):
            return t_ontopic / (t_offtopic + 1)

        event_focus = []
        for u_name, u_stream in stream[['author', 'hashtags']].groupby('author'):
            ontopic_mask = u_stream['hashtags'].apply(lambda t: any(h in event['hashtags'] for h in t))

            tw_ontopic = u_stream[ontopic_mask].shape[0]
            tw_offtopic = u_stream.shape[0] - tw_ontopic

            event_focus.append({
                'user_name': u_name,
                'event_focus': event_focus_alg(tw_ontopic, tw_offtopic)
            })

        df_eventfocus = pd.DataFrame.from_records(event_focus)

        logger.debug(helper.df_tostring(df_eventfocus, 5))

        return df_eventfocus

    @staticmethod
    def __persist_profile(topical_attachment, event_focus, dataset_name):
        logger.info('persist userevent metrics')

        profiles = topical_attachment['user_name'].to_frame()

        for m in [topical_attachment, event_focus]:
            profiles = pd.merge(profiles, m,  # m.drop(columns=['user_id']),
                                how='left', left_on=['user_name'], right_on=['user_name'])

        profile_records = profiles.to_dict('records')
        user_names = [p['user_name'] for p in profile_records]

        try:
            with session_scope() as session:
                # get all users for current dataset
                user_entities = session.query(User)\
                    .filter(User.user_name.in_(user_names)).all()

                # get current event
                event_entity = session.query(Event).filter(Event.name == dataset_name).first()

                profile_entities = []
                for p in profile_records:
                    # get user entities and profile info
                    user_entity = next(filter(lambda x: x.user_name == p['user_name'], user_entities), None)
                    profile = {k: p[k] for k in ('topical_attachment', 'event_focus')}

                    # create profile entity
                    profile_entity = UserEvent(**profile, user=user_entity, event=event_entity)
                    profile_entities.append(profile_entity)

                session.add_all(profile_entities)
            logger.debug('userevent info successfully persisted')
        except IntegrityError:
            logger.debug('userevent info already exists or constraint is violated and could not be added')
