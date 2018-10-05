import pandas as pd
import networkx as nx
import logging
import helper
import numpy as np
import pquality.PartitionQuality as Pq

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self, config, stage_input=None):
        self.config = config
        self.prev_stage_prefix = 'cd'
        self.stage_prefix = 'm'
        self.input = self.__load_input(stage_input)
        self.output = self.__load_output()
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['graph_summary'] = self.__graph_summary(self.input['graph'])
            self.output['partition_summary'] = self.__partition_summary(self.input['graph'], self.input['nodes'])
            self.output['pquality'] = self.__get_pquality(self.input['graph'], self.input['nodes'])
            self.output['cumsum_deg_dist'] = self.__cumsum_deg_dist(self.input['graph'])
            self.output['graph'], self.output['nodes'] = self.__node_metrics(self.input['graph'], self.input['nodes'])
            self.__save_output()

        logger.info(f'END for {self.config.data_filename}')

        return self.output

    def __load_input(self, stage_input):
        logger.info('load input')
        if helper.check_input(['graph', 'nodes', 'edges'], stage_input):
            logger.debug(f'input present')
            return stage_input
        else:
            logger.debug(f'input not present, loading input')

            graph = nx.read_gexf(self.config.get_path(self.prev_stage_prefix, 'graph', 'gexf'), int)
            for n in graph.nodes(data=True):
                n[1].pop('label', None)

            return {
                'graph': graph,
                'nodes': pd.read_csv(self.config.get_path(self.prev_stage_prefix, 'nodes'),
                                     dtype=self.config.data_type['csv_nodes'], index_col=0),
                'edges': pd.read_csv(self.config.get_path(self.prev_stage_prefix, 'edges'),
                                     dtype=self.config.data_type['csv_edges'])
            }

    def __load_output(self):
        logger.info('load output')
        try:
            graph = nx.read_gexf(self.config.get_path(self.stage_prefix, 'graph', 'gexf'), int)
            for n in graph.nodes(data=True):
                n[1].pop('label', None)

            output = {
                'graph_summary': pd.read_csv(self.config.get_path(self.stage_prefix, 'graph_summary')),
                'partition_summary': pd.read_csv(self.config.get_path(self.stage_prefix, 'partition_summary')),
                'pquality': pd.read_csv(self.config.get_path(self.stage_prefix, 'pquality'), index_col='Index'),
                'cumsum_deg_dist': pd.read_csv(self.config.get_path(self.stage_prefix, 'cumsum_deg_dist')),
                'graph': graph,
                'nodes': pd.read_csv(self.config.get_path(self.stage_prefix, 'nodes'),
                                     dtype=self.config.data_type['csv_nodes']),
            }
            logger.debug(f'output present, not executing stage')

            return output
        except IOError as e:
            logger.debug(f'output not present, executing stage: {e}')

            return {}

    def __save_output(self):
        graph_summary_path = self.config.get_path(self.stage_prefix, 'graph_summary')
        partition_summary_path = self.config.get_path(self.stage_prefix, 'partition_summary')
        pquality_path = self.config.get_path(self.stage_prefix, 'pquality')
        cumsum_deg_dist_path = self.config.get_path(self.stage_prefix, 'cumsum_deg_dist')
        nodes_path = self.config.get_path(self.stage_prefix, 'nodes')
        graph_path = self.config.get_path(self.stage_prefix, 'graph', 'gexf')

        self.output['graph_summary'].to_csv(graph_summary_path, index=False)
        self.output['partition_summary'].to_csv(partition_summary_path)
        self.output['pquality'].to_csv(pquality_path)
        self.output['cumsum_deg_dist'].to_csv(cumsum_deg_dist_path)
        self.output['nodes'].to_csv(nodes_path)
        nx.write_gexf(self.output['graph'], graph_path)

        logger.info('save output')
        logger.debug(f'graph summary file path: {graph_summary_path}\n' +
                     helper.df_tostring(self.output['graph_summary']) +
                     f'partition summary file path: {partition_summary_path}\n' +
                     helper.df_tostring(self.output['partition_summary'], 5) +
                     f'pquality file path: {pquality_path}\n' +
                     helper.df_tostring(self.output['pquality']) +
                     f'cumulated sum of degree distribution file path: {cumsum_deg_dist_path}\n' +
                     helper.df_tostring(self.output['cumsum_deg_dist'], 5) +
                     f'nodes file path: {nodes_path}\n' +
                     helper.df_tostring(self.output['nodes'], 5) +
                     f'graph file path: {graph_path}\n' +
                     helper.graph_tostring(self.output['graph'], 3, 3))

    @staticmethod
    def __graph_summary(graph):
        # NaN assortatitvity: https://groups.google.com/forum/#!topic/networkx-discuss/o2zl40LMmqM
        def assortativity(g):
            try:
                return nx.degree_assortativity_coefficient(g)
            except Exception:
                return None

        summary_df = pd.DataFrame(data={
            '# nodes': graph.number_of_nodes(),
            '# edges': graph.number_of_edges(),
            'avg degree': sum([x[1] for x in graph.degree()]) / graph.number_of_nodes(),
            'avg weighted degree': sum([x[1] for x in graph.degree(weight='Weight')]) / graph.number_of_nodes(),
            'density': nx.density(graph),
            'connected': nx.is_weakly_connected(graph),
            'strongly conn components': nx.number_strongly_connected_components(graph),
            'avg clustering': nx.average_clustering(graph),
            'assortativity': assortativity(graph)
        }, index=[0]).round(4)

        logger.info('graph summary')
        logger.debug(f'summary of partition metrics:\n{summary_df.to_string()}\n\n')

        return summary_df

    @staticmethod
    def __partition_summary(graph, nodes):
        communities = [(k, graph.subgraph(tuple(v.values))) for k, v in nodes.groupby('Community').groups.items()]

        c_summary_list = []
        for c_name, c_graph in communities:
            c_summary_df = Metrics.__graph_summary(c_graph)
            c_summary_df['Community'] = c_name
            c_summary_list.append(c_summary_df)

        partition_summary_df = pd.concat(c_summary_list).set_index('Community')

        return partition_summary_df

    @staticmethod
    def __get_pquality(graph, nodes):
        communities = [graph.subgraph(tuple(v.values)) for k, v in nodes.groupby('Community').groups.items()]

        pqualities = [
            ('Internal Density', Pq.internal_edge_density, 1, []),
            ('Edges inside', Pq.internal_edge_density, 1, []),
            ('Normalized Cut', Pq.normalized_cut, 2, []),
            ('Average Degree', Pq.average_internal_degree, 1, []),
            ('FOMD', Pq.fraction_over_median_degree, 1, []),
            ('Expansion', Pq.expansion, 2, []),
            ('Cut Ratio', Pq.cut_ratio, 2, []),
            ('Conductance', Pq.conductance, 2, []),
            ('Maximum-ODF', Pq.max_odf, 2, []),
            ('Average-ODF', Pq.avg_odf, 2, []),
            ('Flake-ODF', Pq.flake_odf, 2, [])
        ]

        m = []
        for pq_name, pq_func, pq_arg_len, pq_values in pqualities:
            for c in communities:
                pq_values.append(pq_func(graph, c) if pq_arg_len == 2 else pq_func(c))
            m.append([pq_name, min(pq_values), max(pq_values), np.mean(pq_values), np.std(pq_values)])

        pquality_df = pd.DataFrame(m, columns=['Index', 'min', 'max', 'avg', 'std']).set_index('Index')

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
                                          columns=['degree', 'cumsum of the number of nodes']).set_index('degree')

        logger.info('cumulated sum of degree')
        logger.debug(helper.df_tostring(cumsum_deg_dist_df, 5))

        return cumsum_deg_dist_df

    @staticmethod
    def __node_metrics(graph, nodes):
        def indegree(g):
            return [(n, g.in_degree(n)) for n in g.nodes]

        def indegree_centrality(g):
            return [(n, ic) for n, ic in nx.in_degree_centrality(g).items()]

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
                edges = [e[2]['Weight'] for e in g.in_edges(n, data=True)]
                hindex_list.append((n, alg_hindex(edges)))

            return hindex_list

        communities = [graph.subgraph(tuple(v.values)) for k, v in nodes.groupby('Community').groups.items()]

        nm_metrics = [
            ('indegree', indegree, []),
            ('indegree centrality', indegree_centrality, []),
            ('hindex', hindex, []),
        ]

        for nm_name, nm_func, nm_values in nm_metrics:
            for c in communities:
                nm_values.extend(nm_func(c))

            nm_attr_name = f'Comm relative {nm_name}'

            # update nodes
            nm_df = pd.DataFrame(nm_values, columns=['index', nm_attr_name]).set_index('index')
            nodes = nodes.join(nm_df)

            # update graph
            nm_attr = {n_id: n_value for n_id, n_value in nm_values}
            nx.set_node_attributes(graph, values=nm_attr, name=nm_attr_name)

        return graph, nodes
