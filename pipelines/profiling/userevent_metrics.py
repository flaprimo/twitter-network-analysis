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
            'nodes': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'nodes'),
                'r_kwargs': {
                    'dtype': {
                        'community': 'uint16',
                        'user_id': 'uint32',
                        'user_name': str,
                        'indegree': 'float32',
                        'indegree_centrality': 'float32',
                        'hindex': 'uint16'
                    }
                },
                'w_kwargs': {'index': False}
            },
            'stream': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'nodes'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'author': str,
                        'date': str,
                        'reply': lambda x: x.strip('[]').split(', '),
                        'language': str,
                        'text': str,
                        'hashtags': lambda x: x.strip('[]').split(', '),
                        'emojis': lambda x: x.strip('[]').split(', '),
                        'urls': lambda x: x.strip('[]').split(', '),
                        'mentions': 'uint32',
                        'replies': 'uint32',
                        'retweets': 'uint32',
                        'likes': 'uint32'
                    }
                },
                'w_kwargs': {'index': False}
            },
            'topicalattachment': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'topicalattachment'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'topical_attachment': 'float32'
                    }
                },
                'w_kwargs': {'index': False}
            },
            'retweetrate': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'retweetrate'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'retweet_rate': 'float32'
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
            # self.output['topicalattachment'] = self.__topical_attachment(self.output['stream'], event)
            # self.output['retweetrate'] = self.__retweet_rate(self.output['stream'], event)

            # if self.config.save_db_output:
            #     self.__persist_profile(self.output['topicalattachment'], self.output['retweetrate'])
            #
            # if self.config.save_io_output:
            #     PipelineIO.save_output(self.output, self.output_format)

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
            streams.append(tw.tw_dynamic_scraper.search(query))

        df_stream = pd.DataFrame.from_records(streams)

        logger.debug(helper.df_tostring(df_stream, 5))

        return df_stream

    @staticmethod
    def __topical_attachment(stream):
        logger.info('compute topical attachment')

        user_names = stream['author'].drop_duplicates().tolist()

        topical_attachments = []
        for u in user_names:
            u_stream = stream[stream['hashtags'].str.contains("hello")]

        # logger.debug(helper.df_tostring(userinfo, 5))

        return None

    @staticmethod
    def __retweet_rate(stream):
        logger.info('compute retweet rate')

        # logger.debug(helper.df_tostring(userinfo, 5))

        return None

    @staticmethod
    def __persist_profile(topical_attachment, retweet_rate):
        logger.info('persist userevent metrics')

        profiles = topical_attachment['user_name'].to_frame()

        for m in [topical_attachment, retweet_rate]:
            profiles = pd.merge(profiles, m.drop(columns=['user_id']),
                                how='left', left_on=['user_name'], right_on=['user_name'])

        profile_records = profiles.to_dict('records')
        user_names = [p['user_name'] for p in profile_records]

        try:
            with session_scope() as session:
                # get all users for current dataset
                user_entities = session.query(User)\
                    .filter(User.user_name.in_(user_names)).all()

                profile_entities = []
                for p in profile_records:
                    # get user entities and profile info
                    user_entity = next(filter(lambda x: x.user_name == p['user_name'], user_entities), None)
                    profile = {k: p[k] for k in ('topical_attachment', 'retweet_rate')}

                    # create profile entity
                    profile_entity = UserEvent(**profile, user=user_entity)
                    profile_entities.append(profile_entity)

                session.add_all(profile_entities)
            logger.debug('userevent info successfully persisted')
        except IntegrityError:
            logger.debug('userevent info already exists or constraint is violated and could not be added')
