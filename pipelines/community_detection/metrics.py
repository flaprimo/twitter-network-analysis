import pandas as pd
import networkx as nx
import logging
import helper
import numpy as np
import pquality.PartitionQuality as Pq
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
            'partition_summary': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'partition_summary'),
                'r_kwargs': {
                    'dtype': {
                        'community': 'uint16',
                        'no_nodes': 'uint16',
                        'no_edges': 'uint16',
                        'avg_degree': 'float32',
                        'avg_weighted_degree': 'float32',
                        'density': 'float32',
                        'connected': bool,
                        'strongly_conn_components': 'uint16',
                        'avg_clustering': 'float32',
                        'assortativity': 'float32'
                    },
                    'index_col': 'community'
                },
                'w_kwargs': {}
            },
            'pquality': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'pquality'),
                'r_kwargs': {
                    'dtype': {
                        'index': str,
                        'min': 'float32',
                        'max': 'float32',
                        'avg': 'float32',
                        'std': 'float32'
                    },
                    'index_col': 'index'
                },
                'w_kwargs': {}
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
            self.output['partition_summary'] = self.__partition_summary(self.input['graph'], self.input['nodes'])
            self.output['pquality'] = self.__get_pquality(self.input['graph'], self.input['nodes'])
            self.output['cumsum_deg_dist'] = self.__cumsum_deg_dist(self.input['graph'])
            self.output['graph'], self.output['nodes'] = self.__node_metrics(self.input['graph'], self.input['nodes'])

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
    def __partition_summary(graph, nodes):
        communities = [(k, graph.subgraph(tuple(v.values)))
                       for k, v in nodes.set_index('user_id').groupby('community').groups.items()]

        c_summary_list = []
        for c_name, c_graph in communities:
            c_summary_df = Metrics.__graph_summary(c_graph)
            c_summary_df['community'] = c_name
            c_summary_list.append(c_summary_df)

        partition_summary_df = pd.concat(c_summary_list).set_index('community')

        return partition_summary_df

    @staticmethod
    def __get_pquality(graph, nodes):
        communities = [graph.subgraph(tuple(v.values))
                       for k, v in nodes.set_index('user_id').groupby('community').groups.items()]

        pqualities = [
            ('internal_density', Pq.internal_edge_density, 1, []),
            ('edges_inside', Pq.internal_edge_density, 1, []),
            ('normalized_cut', Pq.normalized_cut, 2, []),
            ('avg_degree', Pq.average_internal_degree, 1, []),
            ('fomd', Pq.fraction_over_median_degree, 1, []),
            ('expansion', Pq.expansion, 2, []),
            ('cut_ratio', Pq.cut_ratio, 2, []),
            ('conductance', Pq.conductance, 2, []),
            ('max_odf', Pq.max_odf, 2, []),
            ('avg_odf', Pq.avg_odf, 2, []),
            ('flake_odf', Pq.flake_odf, 2, [])
        ]

        m = []
        for pq_name, pq_func, pq_arg_len, pq_values in pqualities:
            for c in communities:
                pq_values.append(pq_func(graph, c) if pq_arg_len == 2 else pq_func(c))
            m.append([pq_name, min(pq_values), max(pq_values), np.mean(pq_values), np.std(pq_values)])

        pquality_df = pd.DataFrame(m, columns=['index', 'min', 'max', 'avg', 'std']).set_index('index')

        logger.info('get partition quality metrics')
        logger.debug(f'summary of partition metrics:\n{pquality_df.to_string()}\n\n')

        return pquality_df

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

    @staticmethod
    def __node_metrics(graph, nodes):
        def indegree(g):
            return [{'user_id': n, 'indegree': g.in_degree(n)} for n in g.nodes]

        def indegree_centrality(g):
            return [{'user_id': n, 'indegree_centrality': ic} for n, ic in nx.in_degree_centrality(g).items()]

        def hindex(g):
            # from https://github.com/kamyu104/LeetCode/blob/master/Python/h-index.py
            def alg_hindex(citations):
                citations.sort(reverse=True)
                h = 0
                for x in citations:
                    if x >= h + 1:
                        h += 1
                    else:
                        break
                return h

            hindex_list = []
            for n in g.nodes:
                edges = [e[2]['weight'] for e in g.in_edges(n, data=True)]
                hindex_list.append({'user_id': n, 'hindex': alg_hindex(edges)})

            return hindex_list

        communities = [(k, graph.subgraph(tuple(v.values)))
                       for k, v in nodes.set_index('user_id').groupby('community').groups.items()]

        nm_metrics = [indegree, indegree_centrality, hindex]

        for nm_func in nm_metrics:
            results = []
            for c_name, c_graph in communities:
                results.extend([{**n, 'community': c_name} for n in nm_func(c_graph)])

            nodes = pd.merge(nodes, pd.DataFrame(results),
                             left_on=['user_id', 'community'], right_on=['user_id', 'community'])

            # update graph
            # nm_attr = {n_id: n_value for n_id, n_value in nm_values}
            # nx.set_node_attributes(graph, values=nm_attr, name=nm_attr_name)
        nodes.sort_values(by=['user_id', 'community'], inplace=True)
        logger.info('apply metrics')
        logger.debug(helper.df_tostring(nodes, 5))

        return graph, nodes
