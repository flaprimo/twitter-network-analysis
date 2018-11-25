import pandas as pd
import networkx as nx
import logging
import helper
from datasources import PipelineIO

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['graph', 'edges', 'nodes'], stage_input, stage_input_format)
        self.output_prefix = 'm'
        self.output_format = {
            'graph_summary': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'graph_summary'),
                'r_kwargs': {
                    'dtype': {
                        'no_nodes': 'uint16',
                        'no_edges': 'uint16',
                        'avg_degree': 'float32',
                        'avg_weighted_degree': 'float32',
                        'density': 'float32',
                        'connected': bool,
                        'strongly_conn_components': 'uint16',
                        'avg_clustering': 'float32',
                        'assortativity': 'float32'
                    }
                },
                'w_kwargs': {'index': False}
            },
            'cumsum_deg_dist': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'cumsum_deg_dist'),
                'r_kwargs': {
                    'dtype': {
                        'degree': 'uint32',
                        'cumsum_of_the_no_of_nodes': 'float32'
                    },
                    'index_col': 'degree'
                },
                'w_kwargs': {}
            },
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
            'graph': {
                'type': 'networkx',
                'path': self.config.get_path(self.output_prefix, 'graph', 'gexf'),
                'r_kwargs': {'node_type': int},
                'w_kwargs': {}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if not self.output:
            self.output['graph_summary'] = self.__graph_summary(self.input['graph'])
            self.output['cumsum_deg_dist'] = self.__cumsum_deg_dist(self.input['graph'])
            self.output['graph'] = self.input['graph']
            self.output['nodes'] = self.input['nodes']
            self.output['edges'] = self.input['edges']

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __graph_summary(graph):
        # NaN assortatitvity: https://groups.google.com/forum/#!topic/networkx-discuss/o2zl40LMmqM
        def assortativity(g):
            try:
                return nx.degree_assortativity_coefficient(g)
            except Exception:
                return None

        summary_df = pd.DataFrame(data={
            'no_nodes': graph.number_of_nodes(),
            'no_edges': graph.number_of_edges(),
            'avg_degree': sum([x[1] for x in graph.degree()]) / graph.number_of_nodes(),
            'avg_weighted_degree': sum([x[1] for x in graph.degree(weight='weight')]) / graph.number_of_nodes(),
            'density': nx.density(graph),
            'connected': nx.is_weakly_connected(graph),
            'strongly_conn_components': nx.number_strongly_connected_components(graph),
            'avg_clustering': nx.average_clustering(graph),
            'assortativity': assortativity(graph)
        }, index=[0]).round(4)

        logger.info('graph summary')
        logger.debug(f'summary of partition metrics:\n{summary_df.to_string()}\n\n')

        return summary_df

    @staticmethod
    def __cumsum_deg_dist(graph):
        import collections

        deg_list = sorted([d for n, d in graph.degree()], reverse=False)
        deg, cnt = zip(*collections.Counter(deg_list).items())

        cumsum = sum(cnt)
        nodes_len = graph.number_of_nodes()

        cumsum_deg_dist_list = []
        for i, (d, c) in enumerate(zip(deg, cnt)):
            cumsum_deg_dist_list.append((d, cumsum / nodes_len))
            cumsum -= c

        cumsum_deg_dist_df = pd.DataFrame(cumsum_deg_dist_list,
                                          columns=['degree', 'cumsum_of_the_no_of_nodes']).set_index('degree')

        logger.info('cumulated sum of degree')
        logger.debug(helper.df_tostring(cumsum_deg_dist_df, 5))

        return cumsum_deg_dist_df
