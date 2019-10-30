import logging
import pandas as pd
import networkx as nx
from pipelines.pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class BipartiteGraph(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_user_network',
                'file_name': 'user_network',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'from_username': str,
                        'to_username': str,
                        'weight': 'uint16'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_hashtag_network',
                'file_name': 'hashtag_network',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'from_hashtag': str,
                        'to_hashtag': str,
                        'weight': 'uint16'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_user_hashtag_network',
                'file_name': 'user_hashtag_network',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_name': str,
                        'hashtag': str,
                        'weight': 'uint16'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_user_hashtag_graph',
                'file_name': 'graph',
                'file_extension': 'gexf',
                'r_kwargs': {
                    'node_type': str
                }
            }
        ]
        tasks = [[self.__get_user_network, self.__get_hashtag_network],
                 self.__get_user_hashtag_network, self.__get_user_hashtag_graph]
        super(BipartiteGraph, self).__init__('bipartite_graph', files, tasks, datasources)

    def __get_user_network(self):
        if not self.datasources.files.exists('bipartite_graph', 'get_user_network', 'user_network', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')[['user_name', 'mentions']]

            # create user edges
            users_network = user_timelines[user_timelines['mentions'].map(lambda m: len(m) > 0)].explode('mentions') \
                .rename(columns={'user_name': 'from_username', 'mentions': 'to_username'}) \
                .drop_duplicates()

            # count users co-occurrences
            users_network['from_username'], users_network['to_username'] = \
                users_network.min(axis=1), users_network.max(axis=1)
            users_network['weight'] = 1
            users_network = users_network.groupby(['from_username', 'to_username']).sum().reset_index()

            self.datasources.files.write(
                users_network, 'bipartite_graph', 'get_user_network', 'user_network', 'csv')

    def __get_hashtag_network(self):
        if not self.datasources.files.exists('bipartite_graph', 'get_hashtag_network', 'hashtag_network', 'csv'):
            hashtags_network = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')['hashtags']

            # pair co-occurred hashtags
            hashtags_network = hashtags_network \
                .map(lambda h_list: [(h1, h2) if h1 < h2 else (h2, h1)
                                     for i, h1 in enumerate(h_list)
                                     for h2 in h_list[:i]])

            # create hashtag edges
            hashtags_network = hashtags_network.explode().dropna()
            hashtags_network = pd.DataFrame(hashtags_network.tolist(),
                                            columns=['from_hashtag', 'to_hashtag'],
                                            index=hashtags_network.index)

            # count hashtags co-occurrences
            hashtags_network['weight'] = 1
            hashtags_network = hashtags_network.groupby(['from_hashtag', 'to_hashtag']).sum().reset_index()

            self.datasources.files.write(
                hashtags_network, 'bipartite_graph', 'get_hashtag_network', 'hashtag_network', 'csv')

    def __get_user_hashtag_network(self):
        if not self.datasources.files.exists(
                'bipartite_graph', 'get_user_hashtag_network', 'user_hashtag_network', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')[['user_name', 'hashtags']]

            # hashtag list per user_name
            hashtags_users_network = user_timelines.groupby('user_name').sum()

            # explode hashtags
            hashtags_users_network = hashtags_users_network.explode('hashtags') \
                .rename(columns={'hashtags': 'hashtag'}).reset_index().dropna()

            hashtags_users_network = hashtags_users_network \
                .groupby(['user_name', 'hashtag']).size().reset_index(name='weight') \
                .sort_values(by=['user_name', 'hashtag'], ascending=True) \
                .reset_index(drop=True)

            self.datasources.files.write(
                hashtags_users_network,
                'bipartite_graph', 'get_user_hashtag_network', 'user_hashtag_network', 'csv')

    def __get_user_hashtag_graph(self):
        if not self.datasources.files.exists('bipartite_graph', 'get_user_hashtag_graph', 'graph', 'gexf'):
            hashtags_users_network = self.datasources.files.read(
                'bipartite_graph', 'get_user_hashtag_network', 'user_hashtag_network', 'csv')
            users_network = self.datasources.files.read(
                'bipartite_graph', 'get_user_network', 'user_network', 'csv')
            hashtags_network = self.datasources.files.read(
                'bipartite_graph', 'get_hashtag_network', 'hashtag_network', 'csv')

            hashtags_users_network['pairs'] = \
                list(zip(hashtags_users_network['user_name'], hashtags_users_network['hashtag']))
            hashtags_users_count_pairs = hashtags_users_network.groupby('weight')['pairs'].apply(list)

            # Create bipartite graph
            bipartite_graph = nx.Graph()

            # Add nodes with the node attribute "bipartite"
            user_nodes = hashtags_users_network['user_name'].tolist()
            hashtags_nodes = hashtags_users_network['hashtag'].tolist()
            bipartite_graph.add_nodes_from(user_nodes, bipartite=0)
            bipartite_graph.add_nodes_from(hashtags_nodes, bipartite=1)

            # Add user-hashtag edges
            for weight, user_hashtag_pair in hashtags_users_count_pairs.items():
                bipartite_graph.add_edges_from(user_hashtag_pair, weight=weight, relation_type='user_hashtag')

            filtered_users_network = \
                users_network[users_network['from_username'].isin(user_nodes) &
                              users_network['to_username'].isin(user_nodes)].reset_index(drop=True)

            filtered_hashtags_network = \
                hashtags_network[hashtags_network['from_hashtag'].isin(hashtags_nodes) &
                                 hashtags_network['to_hashtag'].isin(hashtags_nodes)].reset_index(drop=True)

            # Add user-user and hashtag-hashtag edges
            users_network_edges = filtered_users_network.to_records(index=False).tolist()
            hashtags_network_edges = filtered_hashtags_network.to_records(index=False).tolist()
            bipartite_graph.add_weighted_edges_from(users_network_edges, relation_type='user_user')
            bipartite_graph.add_weighted_edges_from(hashtags_network_edges, relation_type='hashtag_hashtag')

            # remove self loops
            bipartite_graph.remove_edges_from(list(nx.selfloop_edges(bipartite_graph)))

            self.datasources.files.write(
                bipartite_graph, 'bipartite_graph', 'get_user_hashtag_graph', 'graph', 'gexf')
