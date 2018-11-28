import pandas as pd
import logging
from sqlalchemy.exc import IntegrityError
import helper
from datetime import datetime
from datasources import PipelineIO, Tw
from datasources.database.database import session_scope
from datasources.database.model import User

logger = logging.getLogger(__name__)


class ProfileInfo:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['nodes'], stage_input, stage_input_format)
        self.output_prefix = 'pi'
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
            'userinfo': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'userinfo'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'bio': str,
                        'followers': 'uint32',
                        'following': 'uint32',
                        'join_date': str,
                        'likes': 'uint32',
                        'location': str,
                        'name': str,
                        'tweets': 'uint32',
                        'url': str
                    },
                    'parse_dates': ['join_date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d')
                },
                'w_kwargs': {'index': False}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.check_output or not self.output:
            self.output['userinfo'] = self.__get_userinfo(self.input['nodes'])
            self.output['nodes'] = self.input['nodes']
            self.__persist_userinfo(self.output['userinfo'])

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __get_userinfo(nodes):
        logger.info('get user info')
        tw = Tw()

        profiles = [tw.tw_static_scraper.get_user(u)
                    for u in list(set(nodes['user_name']))]

        users = pd.merge(nodes[['user_id', 'user_name']], pd.DataFrame(profiles),
                         how='left', left_on=['user_name'], right_on=['user_name'])
        logger.debug(helper.df_tostring(users))

        return users

    @staticmethod
    def __persist_userinfo(userinfo):
        logger.info('persist userinfo')
        try:
            userinfo['join_date'] = userinfo['join_date'].apply(lambda x: x.date())
        except AttributeError:
            logger.debug('not loaded from disk')

        userinfo_records = userinfo.drop(columns=['user_id']).to_dict('records')
        user_names = userinfo['user_name'].drop_duplicates().tolist()

        try:
            with session_scope() as session:
                # get all users for current dataset
                user_entities = session.query(User.id, User.user_name)\
                    .filter(User.user_name.in_(user_names)).all()

                # update all users with user infos
                for user_entity in user_entities:
                    user = next(filter(lambda x: x['user_name'] == user_entity.user_name, userinfo_records), None)
                    user['id'] = user_entity.id

                session.bulk_update_mappings(User, userinfo_records)
            logger.debug('user info successfully persisted')
        except IntegrityError:
            logger.debug('user info already exists or constraint is violated and could not be added')
