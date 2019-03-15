import logging
import networkx as nx
import pandas as pd
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class HashtagNetwork(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_hashtag_nodes',
                'file_name': 'hashtag_nodes',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'hashtag_id': 'uint32',
                        'hashtag': str
                    },
                    'index_col': 'hashtag_id'
                }
            },
            {
                'stage_name': 'get_hashtag_edges',
                'file_name': 'hashtag_edges',
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
                'stage_name': 'create_graph',
                'file_name': 'graph',
                'file_extension': 'gexf',
                'r_kwargs': {
                    'node_type': int
                }
            }
        ]
        tasks = [self.__get_hashtag_nodes, self.__get_hashtag_edges, self.__create_graph]
        super(HashtagNetwork, self).__init__('hashtag_network', files, tasks, datasources)

    def __get_hashtag_nodes(self):
        if not self.datasources.files.exists('hashtag_network', 'get_hashtag_nodes', 'hashtag_nodes', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'parse_user_timelines', 'user_timelines', 'csv')

            hashtag_nodes = user_timelines['hashtags']
            hashtag_nodes = hashtag_nodes[hashtag_nodes.apply(len) > 0]
            hashtag_nodes = pd.Series(hashtag_nodes.sum()).drop_duplicates().reset_index(drop=True).to_frame('hashtag')
            hashtag_nodes.index.names = ['hashtag_id']

            self.datasources.files.write(hashtag_nodes, 'hashtag_network', 'get_hashtag_nodes', 'hashtag_nodes', 'csv')

    def __get_hashtag_edges(self):
        def flat_list(l):
            return [item for sublist in l for item in sublist]

        def tuple_combinations(l):
            return flat_list([[(h, m) for m in l[i:]] for i, h in enumerate(l[:-1], 1)])

        if not self.datasources.files.exists('hashtag_network', 'get_hashtag_edges', 'hashtag_edges', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'parse_user_timelines', 'user_timelines', 'csv')
            hashtag_nodes = self.datasources.files.read('hashtag_network', 'get_hashtag_nodes', 'hashtag_nodes', 'csv')

            hashtag_edges = user_timelines['hashtags']
            hashtag_edges = hashtag_edges[hashtag_edges.apply(len) > 0]

            # rename hashtags with id
            nodes_dict = pd.Series(hashtag_nodes['hashtag'].index, index=hashtag_nodes['hashtag']).to_dict()
            hashtag_edges = hashtag_edges.map(lambda hashtag_list: [nodes_dict.get(h) for h in hashtag_list])

            # get edges
            hashtag_edges = hashtag_edges.apply(lambda x: tuple_combinations(sorted(x)))
            hashtag_edges = hashtag_edges[hashtag_edges.apply(len) > 0]
            hashtag_edges = pd.DataFrame(hashtag_edges.sum(), columns=['source_id', 'target_id'])

            # add edges weights
            hashtag_edges['weight'] = 1
            hashtag_edges = hashtag_edges.groupby(['source_id', 'target_id']).sum().reset_index() \
                .sort_values(by=['source_id', 'target_id'])

            self.datasources.files.write(hashtag_edges, 'hashtag_network', 'get_hashtag_edges', 'hashtag_edges', 'csv')

    def __create_graph(self):
        if not self.datasources.files.exists('hashtag_network', 'create_graph', 'graph', 'gexf'):
            nodes = self.datasources.files.read('hashtag_network', 'get_hashtag_nodes', 'hashtag_nodes', 'csv')
            edges = self.datasources.files.read('hashtag_network', 'get_hashtag_edges', 'hashtag_edges', 'csv')
            graph = nx.from_pandas_edgelist(edges,
                                            source='source_id', target='target_id', edge_attr=['weight'],
                                            create_using=nx.Graph())
            nx.set_node_attributes(graph, pd.Series(nodes['hashtag']).to_dict(), 'hashtag')

            self.datasources.files.write(graph, 'hashtag_network', 'create_graph', 'graph', 'gexf')
