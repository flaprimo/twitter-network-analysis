import pandas as pd
import logging
from sqlalchemy.exc import IntegrityError
import helper
from datasources import PipelineIO, Tw
from datasources.database.database import session_scope
from datasources.database.model import User, Profile

logger = logging.getLogger(__name__)


class Metrics:
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
                'path': self.config.get_path(self.output_prefix, 'nodes'),
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

        if self.config.check_output or not self.output:
            self.output['followerrank'] = self.__follower_rank(self.input['userinfo'])
            self.__persist_profile(self.output['followerrank'])

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __follower_rank(userinfo):
        logger.info('compute follower rank')

        def follower_rank_alg(followers, following):
            return followers / (followers + following)

        userinfo['follower_rank'] =\
            userinfo.apply(lambda x: follower_rank_alg(x['followers'], x['following']), axis=1)

        logger.debug(helper.df_tostring(userinfo, 5))

        return userinfo[['user_id', 'user_name', 'follower_rank']]

    @staticmethod
    def __persist_profile(follower_rank):
        logger.info('persist profile')

        profiles = follower_rank['user_name'].to_frame()

        prova = follower_rank.copy()
        prova = prova.rename(index=str, columns={'follower_rank': 'topical_attachment'})

        for m in [follower_rank, prova]:
            profiles = pd.merge(profiles, m.drop(columns=['user_id']),
                                how='left', left_on=['user_name'], right_on=['user_name'])

        profile_records = profiles.to_dict('records')

        user_names = profiles['user_name'].drop_duplicates().tolist()

        try:
            with session_scope() as session:
                # get all users for current dataset
                user_entities = session.query(User)\
                    .filter(User.user_name.in_(user_names)).all()

                profile_entities = []
                for p in profile_records:
                    # get user entities and profile info
                    user_entity = next(filter(lambda x: x.user_name == p['user_name'], user_entities), None)
                    profile = {k: p[k] for k in ('follower_rank', 'topical_attachment')}

                    # create profile entity
                    profile_entity = Profile(**profile, user=user_entity)
                    profile_entities.append(profile_entity)

                session.add_all(profile_entities)
            logger.debug('profile info successfully persisted')
        except IntegrityError:
            logger.debug('profile info already exists or constraint is violated and could not be added')
