import pandas as pd
import logging
import helper
from datasources import PipelineIO

logger = logging.getLogger(__name__)


class PreProcessing:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['data'], stage_input, stage_input_format)
        self.output_prefix = 'pp'
        self.output_format = {
            'edges': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'edges'),
                'r_kwargs': {
                    'dtype': {
                        'source_id': 'uint32',
                        'target_id': 'uint32',
                        'weight': 'uint16'
                    },
                },
                'w_kwargs': {'index': False}
            },
            'nodes': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'nodes'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str
                    },
                    'index_col': 'user_id'
                },
                'w_kwargs': {}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['edges'] = self.__drop_columns(self.input['data'])
            self.output['edges'] = self.__merge_duplicates(self.output['edges'])
            self.output['nodes'] = self.__get_nodes(self.output['edges'])
            self.output['edges'] = self.__rename_edges(self.output['nodes'], self.output['edges'])

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.data_filename}')

        return self.output, self.output_format

    @staticmethod
    def __drop_columns(edges):
        columns_tokeep = ['user_from_name', 'user_to_name', 'weights']
        columns_todrop = [c for c in edges.columns.values if c not in columns_tokeep]
        edges = edges[columns_tokeep]
        edges = edges.rename(columns={'user_from_name': 'source_id', 'user_to_name': 'target_id', 'weights': 'weight'})

        logger.info('drop columns')
        logger.debug(f'dropped columns: {columns_todrop}\n' +
                     helper.df_tostring(edges, 5))

        return edges

    @staticmethod
    def __merge_duplicates(edges):
        edges.source_id = edges.source_id.str.lower()
        edges.target_id = edges.target_id.str.lower()

        df_edges_duplicates = edges[edges.duplicated(subset=['source_id', 'target_id'], keep='first')]
        edges = edges.groupby(['source_id', 'target_id']).sum().reset_index()

        logger.info('merge duplicates columns')
        logger.debug(f'number of duplicates: {df_edges_duplicates.shape}\n' +
                     helper.df_tostring(edges, 5))

        return edges

    @staticmethod
    def __get_nodes(edges):
        nodes = pd.concat([edges.source_id, edges.target_id], axis=0).drop_duplicates() \
            .reset_index(drop=True).to_frame('user_name')
        nodes.index.names = ['user_id']

        logger.info('get nodes')
        logger.debug(f'nodes: {nodes.shape}\n' +
                     helper.df_tostring(nodes, 5))

        return nodes

    @staticmethod
    def __rename_edges(nodes, edges):
        nodes_dict = {v: k for k, v in nodes.to_dict()['user_name'].items()}
        edges.source_id = edges.source_id.map(nodes_dict.get)
        edges.target_id = edges.target_id.map(nodes_dict.get)

        logger.info('rename edges')
        logger.debug(f'renamed edges: {edges.shape}\n' +
                     helper.df_tostring(edges, 5))

        return edges
