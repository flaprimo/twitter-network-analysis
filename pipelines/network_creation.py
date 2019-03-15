import logging
import re
import networkx as nx
import pandas as pd
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class NetworkCreation(PipelineBase):
    def __init__(self, datasources, context_name):
        files = [
            {
                'stage_name': 'create_network',
                'file_name': 'network',
                'file_extension': 'csv',
                'file_prefix': context_name,
                'r_kwargs': {
                    'dtype': {
                        'from_username': str,
                        'to_username': str,
                        'text': str,
                    }
                }
            },
            {
                'stage_name': 'create_nodes',
                'file_name': 'nodes',
                'file_extension': 'csv',
                'file_prefix': context_name,
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str
                    },
                    'index_col': 'user_id'
                }
            },
            {
                'stage_name': 'create_edges',
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
                'stage_name': 'create_graph',
                'file_name': 'graph',
                'file_extension': 'gexf',
                'file_prefix': context_name,
                'r_kwargs': {
                    'node_type': int
                }
            }
        ]
        tasks = [self.__create_network, self.__create_nodes, self.__create_edges, self.__create_graph]
        self.context_name = context_name
        super(NetworkCreation, self).__init__('network_creation', files, tasks, datasources)

    def __create_network(self):
        if not self.datasources.files.exists(
                'network_creation', 'create_network', 'network', 'csv', self.context_name):
            stream = self.datasources.files.read(
                'context_detection', 'harvest_context', 'stream', 'json', self.context_name)
            tw_list = []
            for tw in stream:
                # also consider RT as an added value (like +1)?
                # if tw['full_text'].startswith('RT'):
                text = tw['extended_tweet']['full_text'] if tw['truncated'] else tw['text']
                from_username = tw['user']['screen_name'].lower()
                mentions = re.findall(r'@\w+', text)

                for user in mentions:
                    tw_record = {
                        'from_username': from_username,
                        'to_username': user.replace('@', '').lower(),
                        'text': text.replace('\n', ''),
                    }
                    tw_list.append(tw_record)

            tw_df = pd.DataFrame.from_records(tw_list, columns=['from_username', 'to_username', 'text'])

            self.datasources.files.write(
                tw_df, 'network_creation', 'create_network', 'network', 'csv', self.context_name)

    def __create_nodes(self):
        if not self.datasources.files.exists(
                'network_creation', 'create_nodes', 'nodes', 'csv', self.context_name):
            network = self.datasources.files.read(
                'network_creation', 'create_network', 'network', 'csv', self.context_name)
            nodes = pd.concat([network.from_username, network.to_username], axis=0).drop_duplicates()\
                .sort_values().reset_index(drop=True).to_frame('user_name')
            nodes.index.names = ['user_id']

            self.datasources.files.write(
                nodes, 'network_creation', 'create_nodes', 'nodes', 'csv', self.context_name)

    def __create_edges(self):
        if not self.datasources.files.exists(
                'network_creation', 'create_edges', 'edges', 'csv', self.context_name):
            network = self.datasources.files.read(
                'network_creation', 'create_network', 'network', 'csv', self.context_name)
            nodes = self.datasources.files.read(
                'network_creation', 'create_nodes', 'nodes', 'csv', self.context_name)

            # create edges
            edges = network[['from_username', 'to_username']]\
                .rename(columns={'from_username': 'source_id', 'to_username': 'target_id'})

            # assign weights
            edges['weight'] = 1
            edges = edges.groupby(['source_id', 'target_id']).sum().reset_index()\
                .sort_values(by=['source_id', 'target_id'])

            # rename edges
            nodes_dict = pd.Series(nodes['user_name'].index, index=nodes['user_name']).to_dict()
            edges.source_id = edges.source_id.map(nodes_dict.get)
            edges.target_id = edges.target_id.map(nodes_dict.get)

            self.datasources.files.write(
                edges, 'network_creation', 'create_edges', 'edges', 'csv', self.context_name)

    def __create_graph(self):
        if not self.datasources.files.exists(
                'network_creation', 'create_graph', 'graph', 'gexf', self.context_name):
            nodes = self.datasources.files.read(
                'network_creation', 'create_nodes', 'nodes', 'csv', self.context_name)
            edges = self.datasources.files.read(
                'network_creation', 'create_edges', 'edges', 'csv', self.context_name)
            graph = nx.from_pandas_edgelist(edges,
                                            source='source_id', target='target_id', edge_attr=['weight'],
                                            create_using=nx.DiGraph())
            nx.set_node_attributes(graph, pd.Series(nodes['user_name']).to_dict(), 'user_name')

            self.datasources.files.write(
                graph, 'network_creation', 'create_graph', 'graph', 'gexf', self.context_name)
