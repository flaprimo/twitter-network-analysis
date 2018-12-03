import pandas as pd
import logging
import helper
from datasources import PipelineIO
import networkx as nx

logger = logging.getLogger(__name__)


class CreateGraph:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['edges', 'nodes'], stage_input, stage_input_format)
        self.output_prefix = 'cg'
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
            },
            'graph': {
                'type': 'networkx',
                'path': self.config.get_path(self.output_prefix, 'graph', 'gexf'),
                'r_kwargs': {'node_type': int},
                'w_kwargs': {}
            },
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.skip_output_check or not self.output:
            self.output['graph'] = self.__get_graph(self.input['edges'], self.input['nodes'])
            self.output['edges'] = self.input['edges']
            self.output['nodes'] = self.input['nodes']

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __get_graph(edges, nodes):
        graph = nx.from_pandas_edgelist(edges,
                                        source='source_id', target='target_id', edge_attr=['weight'],
                                        create_using=nx.DiGraph())
        nx.set_node_attributes(graph, pd.Series(nodes.user_name).to_dict(), 'user_name')

        logger.info('get graph')
        logger.debug(helper.graph_tostring(graph, 3, 3))

        return graph
