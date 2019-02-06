import logging
from datetime import datetime
import pandas as pd
from sqlalchemy.exc import IntegrityError
from datasources.database.model import User, Profile
from datasources.tw.tw import tw
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class UserMetrics(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name': 'user_info',
                'file_name': 'user_info',
                'file_extension': 'csv',
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
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name':  'remove_nonexistent_users',
                'file_name':  'nodes',
                'file_extension':  'csv',
                'r_kwargs':  {
                    'dtype': {
                        'community': 'uint16',
                        'user_id': 'uint32',
                        'user_name': str,
                        'indegree': 'float32',
                        'indegree_centrality': 'float32',
                        'hindex': 'uint16'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'remove_nonexistent_users',
                'file_name': 'edges',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'source_id': 'uint32',
                        'target_id': 'uint32',
                        'weight': 'uint16'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'remove_nonexistent_users',
                'file_name': 'graph',
                'file_extension': 'gexf',
                'r_kwargs': {
                    'node_type': int
                }
            },
            {
                'stage_name': 'follower_rank',
                'file_name': 'profiles',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'follower_rank': 'float32'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__user_info, self.__remove_nonexistent_users, self.__follower_rank]
        super(UserMetrics, self).__init__('user_metrics', files, tasks, datasources, file_prefix)

    def __user_info(self):
        if not self.datasources.files.exists(
                'user_metrics', 'user_info', 'user_info', 'csv', self.context_name):
            nodes = self.datasources.files.read(
                'community_detection_metrics', 'node_metrics', 'nodes', 'csv', self.context_name)

            unique_users = nodes[['user_id', 'user_name']].drop_duplicates()

            profiles = [x for x in
                        (tw.tw_static_scraper.get_user(u) for u in unique_users['user_name'].tolist()) if x]

            userinfo = pd.merge(pd.DataFrame(profiles), unique_users,
                                how='left', left_on=['user_name'], right_on=['user_name'])

            try:
                userinfo['join_date'] = userinfo['join_date'].apply(lambda x: x.date())
            except AttributeError:
                logger.debug('not loaded from disk')

            userinfo_records = userinfo.drop(columns=['user_id']).to_dict('records')
            user_names = userinfo['user_name'].drop_duplicates().tolist()

            try:
                with self.datasources.database.session_scope() as session:
                    # get all users for current dataset
                    user_entities = session.query(User.id, User.user_name) \
                        .filter(User.user_name.in_(user_names)).all()

                    # update all users with user infos
                    for user_entity in user_entities:
                        user = next(filter(lambda x: x['user_name'] == user_entity.user_name, userinfo_records), None)
                        user['id'] = user_entity.id

                    session.bulk_update_mappings(User, userinfo_records)
                logger.debug('user info successfully persisted')
            except IntegrityError:
                logger.debug('user info already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                userinfo, 'user_metrics', 'user_info', 'user_info', 'csv', self.context_name)

    def __remove_nonexistent_users(self):
        if not self.datasources.files.exists(
                'user_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name) or\
            not self.datasources.files.exists(
                'user_metrics', 'remove_nonexistent_users', 'edges', 'csv', self.context_name) or\
            not self.datasources.files.exists(
                'user_metrics', 'remove_nonexistent_users', 'graph', 'gexf', self.context_name):

            user_info = self.datasources.files.read(
                'user_metrics', 'user_info', 'user_info', 'csv', self.context_name)
            nodes = self.datasources.files.read(
                'community_detection_metrics', 'node_metrics', 'nodes', 'csv', self.context_name)
            edges = self.datasources.files.read(
                'community_detection', 'remove_lone_nodes_from_edges', 'edges', 'csv', self.context_name)
            graph = self.datasources.files.read(
                'community_detection', 'add_communities_to_graph', 'graph', 'gexf', self.context_name)

            nodes = nodes[nodes.index.isin(user_info['user_id'])]
            edges = edges[edges.source_id.isin(user_info['user_id']) & edges.target_id.isin(user_info['user_id'])]
            graph.remove_nodes_from(graph.nodes - user_info['user_id'].tolist())

            self.datasources.files.write(
                nodes, 'user_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name)
            self.datasources.files.write(
                edges, 'user_metrics', 'remove_nonexistent_users', 'edges', 'csv', self.context_name)
            self.datasources.files.write(
                graph, 'user_metrics', 'remove_nonexistent_users', 'graph', 'gexf', self.context_name)

    def __follower_rank(self):
        if not self.datasources.files.exists(
                'user_metrics', 'follower_rank', 'profiles', 'csv', self.context_name):
            user_info = self.datasources.files.read(
                'user_metrics', 'user_info', 'user_info', 'csv', self.context_name)

            # normalized follower ratio from https://doi.org/10.1016/j.ipm.2016.04.003
            def follower_rank_alg(followers, following):
                try:
                    follower_rank = followers / (followers + following)
                except ZeroDivisionError:
                    follower_rank = 0
                return follower_rank

            user_info['follower_rank'] =\
                user_info.apply(lambda x: follower_rank_alg(x['followers'], x['following']), axis=1)

            profile_records = user_info.drop(columns=['user_id']).to_dict('records')
            user_names = [p['user_name'] for p in profile_records]

            try:
                with self.datasources.database.session_scope() as session:
                    # get all users for current dataset
                    user_entities = session.query(User) \
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

            self.datasources.files.write(
                user_info[['user_id', 'user_name', 'follower_rank']],
                'user_metrics', 'follower_rank', 'profiles', 'csv', self.context_name)
