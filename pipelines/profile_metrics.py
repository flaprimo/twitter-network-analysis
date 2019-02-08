import logging
from datetime import datetime
import pandas as pd
from datasources.tw.tw import tw
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class ProfileMetrics(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name': 'profile_info',
                'file_name': 'profile_info',
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
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d'),
                    'index_col': 'user_id'
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
                    },
                    'index_col': 'user_id'
                }
            }
        ]
        tasks = [self.__profile_info, self.__remove_nonexistent_users, self.__follower_rank]
        super(ProfileMetrics, self).__init__('profile_metrics', files, tasks, datasources, file_prefix)

    def __profile_info(self):
        if not self.datasources.files.exists(
                'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name):
            nodes = self.datasources.files.read(
                'community_detection_metrics', 'node_metrics', 'nodes', 'csv', self.context_name)

            unique_users = nodes[['user_id', 'user_name']].drop_duplicates()

            profiles = [x for x in
                        (tw.tw_static_scraper.get_user(u) for u in unique_users['user_name'].tolist()) if x]

            userinfo = pd.merge(pd.DataFrame(profiles), unique_users,
                                how='left', left_on=['user_name'], right_on=['user_name']).set_index('user_id')

            self.datasources.files.write(
                userinfo, 'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)

    def __remove_nonexistent_users(self):
        if not self.datasources.files.exists(
                'profile_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name) or\
            not self.datasources.files.exists(
                'profile_metrics', 'remove_nonexistent_users', 'edges', 'csv', self.context_name) or\
            not self.datasources.files.exists(
                'profile_metrics', 'remove_nonexistent_users', 'graph', 'gexf', self.context_name):

            profile_info = self.datasources.files.read(
                'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)
            nodes = self.datasources.files.read(
                'community_detection_metrics', 'node_metrics', 'nodes', 'csv', self.context_name)
            edges = self.datasources.files.read(
                'community_detection', 'remove_lone_nodes_from_edges', 'edges', 'csv', self.context_name)
            graph = self.datasources.files.read(
                'community_detection', 'add_communities_to_graph', 'graph', 'gexf', self.context_name)

            nodes = nodes[nodes.user_id.isin(profile_info.index)]
            edges = edges[edges.source_id.isin(profile_info.index) & edges.target_id.isin(profile_info.index)]
            graph.remove_nodes_from(set(graph.nodes) - set(profile_info.index.tolist()))

            self.datasources.files.write(
                nodes, 'profile_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name)
            self.datasources.files.write(
                edges, 'profile_metrics', 'remove_nonexistent_users', 'edges', 'csv', self.context_name)
            self.datasources.files.write(
                graph, 'profile_metrics', 'remove_nonexistent_users', 'graph', 'gexf', self.context_name)

    def __follower_rank(self):
        if not self.datasources.files.exists(
                'profile_metrics', 'follower_rank', 'profiles', 'csv', self.context_name):
            profile_info = self.datasources.files.read(
                'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)

            # normalized follower ratio from https://doi.org/10.1016/j.ipm.2016.04.003
            def follower_rank_alg(followers, following):
                try:
                    follower_rank = followers / (followers + following)
                except ZeroDivisionError:
                    follower_rank = 0
                return follower_rank

            profile_info['follower_rank'] = profile_info[['followers', 'following']]\
                .apply(lambda x: follower_rank_alg(x['followers'], x['following']), axis=1)
            profile_info = profile_info[['user_name', 'follower_rank']]

            # NOT INCLUDED
            # profile_records = profile_info.drop(columns=['user_id']).to_dict('records')
            # user_names = [p['user_name'] for p in profile_records]
            #
            # try:
            #     with self.datasources.database.session_scope() as session:
            #         # get all users for current dataset
            #         user_entities = session.query(User) \
            #             .filter(User.user_name.in_(user_names)).all()
            #
            #         profile_entities = []
            #         for p in profile_records:
            #             # get user entities and profile info
            #             user_entity = next(filter(lambda x: x.user_name == p['user_name'], user_entities), None)
            #
            #             # create profile entity
            #             profile_entity = Profile(follower_rank=p['follower_rank'], user=user_entity)
            #             profile_entities.append(profile_entity)
            #
            #         session.add_all(profile_entities)
            #     logger.debug('profile metrics successfully persisted')
            # except IntegrityError:
            #     logger.debug('profile metrics already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                profile_info,
                'profile_metrics', 'follower_rank', 'profiles', 'csv', self.context_name)