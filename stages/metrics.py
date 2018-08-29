import pandas as pd
import networkx as nx
import numpy as np
import pquality.PartitionQuality as Pq
import logging
from tqdm import tqdm
import helper

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self, config, nodes, edges):
        self.config = config
        self.communities = list(nodes.columns)
        self.g = nx.from_pandas_edgelist(edges,
                                         source='Source', target='Target', edge_attr=['Weight'],
                                         create_using=nx.DiGraph())
        for c in self.communities:
            nx.set_node_attributes(self.g, pd.Series(nodes[c]).to_dict(), c)
        self.c_subgraphs = self.__get_community_subgraphs()
        self.scores = {}

    def execute(self):
        logging.info('METRICS')

        self.__pquality()
        self.__iter_metric_c(self.__add_hindex, 'hindex')
        self.__iter_metric_c(self.__add_indegree, 'indegree')

    def __get_community_subgraphs(self):
        c_subgraphs = []

        for c in tqdm(self.communities):
            c_nodes = [x for x, y in self.g.nodes(data=True) if y[c]]
            c_subgraph = nx.DiGraph(self.g.subgraph(c_nodes))

            # remove node attributes to keep memory low
            for n in c_subgraph.nodes(data=True):
                for com in self.communities:
                    n[1].pop(com, None)

            c_subgraphs.append((c, c_subgraph))

        logging.info('get community subgraphs\n'
                     f'  number of communities: {len(self.communities)}\n'
                     f'  community list: {self.communities}\n'
                     f'  communities (only first node for each community is shown):'
                     f'{[(c[0], list(c[1].nodes(data=True))[1]) for c in c_subgraphs]}\n\n')

        return c_subgraphs

    def __pquality(self):
        n_cut, ied, aid, fomd, ex, cr, cond, nedges, modf, aodf, flake, tpr = ([] for _ in range(12))

        for c_label, community in tqdm(self.c_subgraphs):
            n_cut.append(Pq.normalized_cut(self.g, community))
            ied.append(Pq.internal_edge_density(community))
            aid.append(Pq.average_internal_degree(community))
            fomd.append(Pq.fraction_over_median_degree(community))
            ex.append(Pq.expansion(self.g, community))
            cr.append(Pq.cut_ratio(self.g, community))
            nedges.append(Pq.edges_inside(community))
            cond.append(Pq.conductance(self.g, community))
            modf.append(Pq.max_odf(self.g, community))
            aodf.append(Pq.avg_odf(self.g, community))
            flake.append(Pq.flake_odf(self.g, community))

        m = [
            ['Internal Density', min(ied), max(ied), np.mean(ied), np.std(ied)],
            ['Edges inside', min(nedges), max(nedges), np.mean(nedges), np.std(nedges)],
            ['Average Degree', min(aid), max(aid), np.mean(aid), np.std(aid)],
            ['FOMD', min(fomd), max(fomd), np.mean(fomd), np.std(fomd)],
            ['Expansion', min(ex), max(ex), np.mean(ex), np.std(ex)],
            ['Cut Ratio', min(cr), max(cr), np.mean(cr), np.std(cr)],
            ['Conductance', min(cond), max(cond), np.mean(cond), np.std(cond)],
            ['Normalized Cut', min(n_cut), max(n_cut), np.mean(n_cut), np.std(n_cut)],
            ['Maximum-ODF', min(modf), max(modf), np.mean(modf), np.std(modf)],
            ['Average-ODF', min(aodf), max(aodf), np.mean(aodf), np.std(aodf)],
            ['Flake-ODF', min(flake), max(flake), np.mean(flake), np.std(flake)]
        ]

        self.scores['pquality'] = pd.DataFrame(m, columns=['Index', 'min', 'max', 'avg', 'std']).set_index('Index')

        logging.info('get partition quality metrics\n'
                     f'summary of partition metrics:\n{self.scores["pquality"].to_string()}\n\n')

    @staticmethod
    def __add_hindex(g):
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
    def __add_indegree(g):
        return [(n, g.in_degree(n)) for n in g.nodes]

    def __iter_metric_c(self, metric, metric_name):
        c_metric_list = []
        for c_label, c in self.c_subgraphs:
            c_metric = pd.DataFrame(data=metric(c), columns=['Id', metric_name]).set_index('Id') \
                .sort_values(by=[metric_name], ascending=False)
            c_metric[c_label] = True
            c_metric_list.append(c_metric)

        self.scores[metric_name] = pd.concat(c_metric_list, sort=False).fillna(False)

        logging.info(f'executed {metric_name.upper()} metric\n'
                     f'(show first 5 nodes per community):\n{helper.df_tostring(self.scores[metric_name])}\n')

    def metric_top(self, metric_name, n=10):
        df_top = self.scores[metric_name]
        df_top_list = [(c_label, df_top[df_top[c_label]][metric_name].head(n)) for c_label in self.communities]

        logging.info(f'top {n} for {metric_name.upper()}\n' +
                     ''.join([f'{c[0]}\n{helper.df_tostring(c[1], 10)}\n' for c in df_top_list]))

        return df_top_list

    def save(self):
        for metric_name, metric_df in self.scores.items():
            path = self.config.get_path('m', metric_name)
            metric_df.to_csv(path)
            logger.info(f'save {metric_name} csv\n'
                        f'  path: {path}\n' +
                        helper.df_tostring(metric_df))
