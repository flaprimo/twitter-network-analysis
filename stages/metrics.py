import pandas as pd
import networkx as nx
import numpy as np
import pquality.PartitionQuality as Pq
import logging
from tqdm import tqdm_notebook as tqdm
import helper

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self, config, nodes=None, edges=None):
        self.config = config
        logger.info(f'METRICS: {self.config.data_filename} - '
                    f'e:{self.config.demon["epsilon"]} mcs:{self.config.demon["min_community_size"]}')
        self.edges = edges if edges else self.__load_edges()
        self.nodes = nodes if nodes else self.__load_nodes()

        self.communities = list(self.nodes.columns)
        self.g = self.__get_graph()
        self.c_subgraphs = self.__get_community_subgraphs()
        self.scores = {}

    def execute(self):
        logger.info('execute')
        self.__pquality()
        self.__iter_metric_c(self.__hindex, 'hindex')
        self.__iter_metric_c(self.__indegree, 'indegree')

    def __load_edges(self):
        edges_path = self.config.get_path('cd', 'edges')
        edges = pd.read_csv(edges_path,
                            dtype=self.config.data_type['csv_edges'])

        logger.info('load edges csv')
        logger.debug(f'edges file path: {edges_path}\n' +
                     helper.df_tostring(edges, 5))

        return edges

    def __load_nodes(self):
        nodes_path = self.config.get_path('cd', 'nodes')
        nodes = pd.read_csv(nodes_path,
                            dtype=self.config.data_type['csv_nodes'],
                            index_col='Id')

        logger.info('load nodes csv')
        logger.debug(f'  nodes file path: {nodes_path}\n' +
                     helper.df_tostring(nodes, 5))

        return nodes

    def __get_graph(self):
        graph = nx.from_pandas_edgelist(self.edges,
                                        source='Source', target='Target', edge_attr=['Weight'],
                                        create_using=nx.DiGraph())
        for c in self.communities:
            nx.set_node_attributes(graph, pd.Series(self.nodes[c]).to_dict(), c)

        logger.info('get graph')

        return graph

    def __get_community_subgraphs(self):
        c_subgraphs = []

        print(f'get community subgraphs for '
              f'e:{self.config.demon["epsilon"]} mcs:{self.config.demon["min_community_size"]}')
        for c in tqdm(self.communities):
            c_nodes = [x for x, y in self.g.nodes(data=True) if y[c]]
            c_subgraph = nx.DiGraph(self.g.subgraph(c_nodes))

            # remove node attributes to keep memory low
            for n in c_subgraph.nodes(data=True):
                for com in self.communities:
                    n[1].pop(com, None)

            c_subgraphs.append((c, c_subgraph))

        logger.info('get community subgraphs')
        logger.debug(f'  number of communities: {len(self.communities)}\n'
                     f'  community list: {self.communities}\n'
                     f'  communities (only first node for each community is shown):'
                     f'{[(c[0], list(c[1].nodes(data=True))[1]) for c in c_subgraphs]}\n\n')

        return c_subgraphs

    def __pquality(self):
        pq = [
            ('Normalized Cut', Pq.normalized_cut, 2, []),
            ('Edges inside', Pq.internal_edge_density, 1, []),
            ('Average Degree', Pq.average_internal_degree, 1, []),
            ('FOMD', Pq.fraction_over_median_degree, 1, []),
            ('Expansion', Pq.expansion, 2, []),
            ('Cut Ratio', Pq.cut_ratio, 2, []),
            ('Edges inside', Pq.edges_inside, 1, []),
            ('Conductance', Pq.conductance, 2, []),
            ('Maximum-ODF', Pq.max_odf, 2, []),
            ('Average-ODF', Pq.avg_odf, 2, []),
            ('Flake-ODF', Pq.flake_odf, 2, [])
        ]

        print(f'get pquality for '
              f'e:{self.config.demon["epsilon"]} mcs:{self.config.demon["min_community_size"]}')
        m = []
        tqdm_pq = tqdm(pq)
        for func in tqdm_pq:
            tqdm_pq.set_description(func[0])
            tqdm_pq.refresh()
            for c_label, community in self.c_subgraphs:
                func[3].append(func[1](self.g, community) if func[2] == 2 else func[1](community))
            m.append([func[0], min(func[3]), max(func[3]), np.mean(func[3]), np.std(func[3])])

        self.scores['pquality'] = pd.DataFrame(m, columns=['Index', 'min', 'max', 'avg', 'std']).set_index('Index')

        logger.info('get partition quality metrics')
        logger.debug(f'summary of partition metrics:\n{self.scores["pquality"].to_string()}\n\n')

    @staticmethod
    def __hindex(g):
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

        hindex = []
        for n in g.nodes:
            edges = [e[2]['Weight'] for e in g.in_edges(n, data=True)]
            hindex.append((n, alg_hindex(edges)))

        return hindex

    @staticmethod
    def __indegree(g):
        return [(n, g.in_degree(n)) for n in g.nodes]

    def __iter_metric_c(self, metric, metric_name):
        c_metric_list = []
        for c_label, c in self.c_subgraphs:
            c_metric = pd.DataFrame(data=metric(c), columns=['Id', metric_name]).set_index('Id') \
                .sort_values(by=[metric_name], ascending=False)
            c_metric[c_label] = True
            c_metric_list.append(c_metric)

        self.scores[metric_name] = pd.concat(c_metric_list, sort=False).fillna(False)

        logger.info(f'executed {metric_name.upper()} metric')
        logger.debug(f'(show first 5 nodes per community):\n{helper.df_tostring(self.scores[metric_name], 5)}\n')

    def metric_top_values(self, metric_name, n=10):
        df_top = self.scores[metric_name]
        df_top_list = [(c_label, df_top[df_top[c_label]][metric_name].head(n)) for c_label in self.communities]

        logger.info(f'top {n} for {metric_name.upper()}')
        logger.debug(''.join([f'{c[0]}\n{helper.df_tostring(c[1], 10)}\n' for c in df_top_list]))

        return df_top_list

    @staticmethod
    def metric_top(communities):
        ranked_list = [c.index.rename(c_label).to_frame().reset_index(drop=True) for c_label, c in communities]
        ranked_df = pd.concat(ranked_list, axis=1)

        logger.info('top users for each community')
        logger.debug(helper.df_tostring(ranked_df, 10))

        return ranked_df

    @staticmethod
    def compare_metric_top(p1, p2, method='kendall'):
        m = np.zeros((p1.shape[1], p2.shape[1]))

        for c1 in p1:
            for c2 in p2:
                c1_name = int(c1[2:])
                c2_name = int(c2[2:])
                m[c1_name][c2_name] = p1[c1].corr(p2[c2], method)

        scores_matrix = pd.DataFrame(m, index=p1.columns, columns=p2.columns)
        scores_list = scores_matrix.unstack().sort_values(ascending=False)

        logger.info(f'executed {method} ranking on top users for each community')
        logger.debug(helper.df_tostring(scores_matrix))
        logger.debug(scores_list)

        return scores_matrix, scores_list

    @staticmethod
    def graph_info(g):
        logger.info('graph info')

        df = pd.DataFrame(data={
            '# nodes': g.number_of_nodes(),
            '# edges': g.number_of_edges(),
            'avg degree': sum([x[1] for x in g.degree()]) / g.number_of_nodes(),
            'avg weighted degree': sum([x[1] for x in g.degree(weight='Weight')]) / g.number_of_nodes(),
            'density': nx.density(g),
            'connected': nx.is_weakly_connected(g),
            'strongly conn components': nx.number_strongly_connected_components(g),
            'avg clustering': nx.average_clustering(g),
            'assortativity': nx.degree_assortativity_coefficient(g)
        }, index=[0]).round(4)

        return df

    def cumsum_deg_dist(self):
        import collections

        deg_list = sorted([d for n, d in self.g.degree()], reverse=False)  # degree sequence
        deg_count = collections.Counter(deg_list)
        deg, cnt = zip(*deg_count.items())

        cum_sum_list = []
        cum_sum = self.g.number_of_nodes()
        for i, d in enumerate(deg):
            cum_sum -= cnt[i]
            deg_dist_cum = cum_sum / self.g.number_of_nodes()
            cum_sum_list.append((d, round(deg_dist_cum, 2)))

        logger.info('executed cumulated sum degree distribution')
        logger.debug(cum_sum_list)

        return cum_sum_list

    def communities_size(self):
        df = pd.DataFrame({c_name: c_graph.number_of_nodes() for c_name, c_graph in self.c_subgraphs},
                          dtype='uint8', index=['number of nodes'])

        logger.info('executed community size')
        logger.debug(helper.df_tostring(df))

        return df

    def save(self):
        for metric_name, metric_df in self.scores.items():
            path = self.config.get_path('m', metric_name)
            metric_df.to_csv(path)

            logger.info(f'save {metric_name} csv')
            logger.debug(f'  metric file path: {path}\n' +
                         helper.df_tostring(metric_df, 5))
