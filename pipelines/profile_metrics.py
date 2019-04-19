import logging
from datetime import datetime
import pandas as pd
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class ProfileMetrics(PipelineBase):
    def __init__(self, datasources, context_name):
        files = [
            {
                'stage_name': 'profile_info',
                'file_name': 'profile_info',
                'file_extension': 'csv',
                'file_prefix': context_name,
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
                'stage_name': 'remove_nonexistent_users',
                'file_name': 'nodes',
                'file_extension': 'csv',
                'file_prefix': context_name,
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
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'remove_nonexistent_users',
                'file_name': 'edges',
                'file_extension': 'csv',
                'file_prefix': context_name,
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
                'file_prefix': context_name,
                'r_kwargs': {
                    'node_type': int
                }
            },
            {
                'stage_name': 'follower_rank',
                'file_name': 'profiles',
                'file_extension': 'csv',
                'file_prefix': context_name,
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
        tasks = [self.__profile_info, [self.__remove_nonexistent_users, self.__follower_rank]]
        self.context_name = context_name
        super(ProfileMetrics, self).__init__('profile_metrics', files, tasks, datasources)

    def __profile_info(self):
        if not self.datasources.files.exists(
                'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name):
            nodes = self.datasources.files.read(
                'community_detection_metrics', 'node_metrics', 'nodes', 'csv', self.context_name)

            unique_users = nodes[['user_id', 'user_name']].drop_duplicates()

            profiles = self.datasources.tw_api.get_user_profiles(unique_users['user_name'].tolist())

            userinfo = pd.merge(pd.DataFrame.from_records(profiles), unique_users,
                                how='left', left_on=['user_name'], right_on=['user_name']).set_index('user_id')

            self.datasources.files.write(
                userinfo, 'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)

    def __remove_nonexistent_users(self):
        if not self.datasources.files.exists(
                'profile_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name) or \
                not self.datasources.files.exists(
                    'profile_metrics', 'remove_nonexistent_users', 'edges', 'csv', self.context_name) or \
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

            profile_info['follower_rank'] = profile_info[['followers', 'following']] \
                .apply(lambda x: follower_rank_alg(x['followers'], x['following']), axis=1)
            profile_info = profile_info[['user_name', 'follower_rank']]

            self.datasources.files.write(
                profile_info, 'profile_metrics', 'follower_rank', 'profiles', 'csv', self.context_name)
