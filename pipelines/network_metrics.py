import logging
import networkx as nx
import pandas as pd
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class NetworkMetrics(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name': 'graph_summary',
                'file_name': 'graph_summary',
                'file_extension': 'csv',
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
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'cumsum_deg_dist',
                'file_name': 'cumsum_deg_dist',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'degree': 'uint32',
                        'cumsum_of_the_no_of_nodes': 'float32'
                    },
                    'index_col': 'degree'
                }
            }
        ]
        tasks = [self.__graph_summary, self.__cumsum_deg_dist]
        super(NetworkMetrics, self).__init__('network_metrics', files, tasks, datasources, file_prefix)

    def __graph_summary(self):
        if not self.datasources.files.exists(
                'network_metrics', 'graph_summary', 'graph_summary', 'csv', self.context_name):
            graph = self.datasources.files.read(
                'network_creation', 'create_graph', 'graph', 'gexf', self.context_name)

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

            self.datasources.files.write(
                summary_df, 'network_metrics', 'graph_summary', 'graph_summary', 'csv', self.context_name)

    def __cumsum_deg_dist(self):
        if not self.datasources.files.exists(
                'network_metrics', 'cumsum_deg_dist', 'cumsum_deg_dist', 'csv', self.context_name):
            graph = self.datasources.files.read(
                'network_creation', 'create_graph', 'graph', 'gexf', self.context_name)
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

            self.datasources.files.write(
                cumsum_deg_dist_df, 'network_metrics', 'cumsum_deg_dist', 'cumsum_deg_dist', 'csv', self.context_name)
