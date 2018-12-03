import pandas as pd
import logging
from sqlalchemy.exc import IntegrityError
import helper
from datasources import PipelineIO
from datasources.database.database import session_scope
from datasources.database.model import User, Profile

logger = logging.getLogger(__name__)


class ProfileMetrics:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['nodes', 'userinfo'], stage_input, stage_input_format)
        self.output_prefix = 'm'
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
            'followerrank': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'followerrank'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'follower_rank': 'float32'
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
            self.output['followerrank'] = self.__follower_rank(self.input['userinfo'])

            if self.config.save_db_output:
                self.__persist_profile(self.output['followerrank'])

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __follower_rank(userinfo):
        logger.info('compute follower rank')

        # normalized follower ratio from https://doi.org/10.1016/j.ipm.2016.04.003
        def follower_rank_alg(followers, following):
            try:
                follower_rank = followers / (followers + following)
            except ZeroDivisionError:
                follower_rank = 0
            return follower_rank

        userinfo['follower_rank'] =\
            userinfo.apply(lambda x: follower_rank_alg(x['followers'], x['following']), axis=1)

        logger.debug(helper.df_tostring(userinfo, 5))

        return userinfo[['user_id', 'user_name', 'follower_rank']]

    @staticmethod
    def __persist_profile(follower_rank):
        logger.info('persist profile metrics')

        profile_records = follower_rank.drop(columns=['user_id']).to_dict('records')
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

                    # create profile entity
                    profile_entity = Profile(follower_rank=p['follower_rank'], user=user_entity)
                    profile_entities.append(profile_entity)

                session.add_all(profile_entities)
            logger.debug('profile metrics successfully persisted')
        except IntegrityError:
            logger.debug('profile metrics already exists or constraint is violated and could not be added')
