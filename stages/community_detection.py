import pandas as pd
import networkx as nx
import demon as d
import re
import os
import logging
import helper

logger = logging.getLogger(__name__)


class CommunityDetection:
    def __init__(self, config, edges):
        self.config = config
        self.edges = edges
        self.graph = nx.from_pandas_edgelist(self.edges,
                                             source='Source', target='Target', edge_attr=['Weight'],
                                             create_using=nx.DiGraph())

    def execute(self):
        logging.info('PRE-PROCESSING')
        self.__execute_demon()
        self.__load_demon_communities()
        self.__handle_lone_nodes()

    def __execute_demon(self):
        path_communities = self.config.get_path('cd', 'communities', 'txt')

        if not os.path.isfile(path_communities):
            logger.info('execute DEMON\n'
                        f'community file will be saved in: {path_communities}\n')
            dm = d.Demon(graph=self.graph,
                         epsilon=self.config.demon['epsilon'],
                         min_community_size=self.config.demon['min_community_size'],
                         file_output=path_communities)
            dm.execute()
        else:
            logger.info('NOT execute DEMON\n'
                        f'community file for the given settings already exists: {path_communities}\n')

    def __load_demon_communities(self):
        def parse_demon_communities():
            communities = []
            with open(self.config.get_path('cd', 'communities', 'txt'), 'r') as f:
                for i, line in enumerate(f):
                    c = line.split("\t", 1)[0]
                    u = re.findall("'([^']*)'", line)
                    for n in u:
                        communities.append((c, n))

            logger.debug('parse demon communities\n'
                         f'  node-community pairs (first 3 pairs): {communities[:3]}\n')

            return communities

        df_communities = pd.DataFrame.from_records(parse_demon_communities(),
                                                   columns=['community', 'Id'], index='Id')

        # Merge users from different communities together
        dummies = pd.get_dummies(df_communities['community'], prefix="C", dtype=bool)
        combine = pd.concat([df_communities, dummies], axis=1)

        self.nodes = combine.groupby(df_communities.index).sum()

        logger.info('load demon communities\n' +
                    helper.df_tostring(self.nodes))

    def __handle_lone_nodes(self):
        def add_lone_nodes():
            all_nodes = pd.concat([self.edges.Source, self.edges.Target]) \
                .drop_duplicates().to_frame('Id').set_index('Id')
            self.nodes = pd.concat([self.nodes, all_nodes], axis=1, sort=True).fillna(False)

            logger.info('add lone nodes\n'
                        f'  lone nodes number: {all_nodes.shape[0] - self.nodes.shape[0]}\n' +
                        helper.df_tostring(self.nodes))

        def rm_lone_edges():
            edges_before = self.edges
            self.edges = self.edges[self.edges.Source.isin(self.nodes.index) &
                                    self.edges.Target.isin(self.nodes.index)]

            logger.info('rm lone edges\n'
                        f'  shape before: {edges_before.shape}\n'
                        f'  shape after: {self.edges.shape}\n'
                        f'  lone edges number: {edges_before.shape[0] - self.edges.shape[0]}\n' +
                        helper.df_tostring(self.edges))

        if self.config.keep_lone_nodes:
            add_lone_nodes()
        else:
            rm_lone_edges()

    def save(self):
        self.nodes.to_csv(self.config.get_path('cd', 'nodes'))
        self.edges.to_csv(self.config.get_path('cd', 'edges'), index=False)

        logger.info('save csv\n'
                    f'  path: {self.config.get_path("cd", "nodes")}\n' +
                    helper.df_tostring(self.nodes) +
                    f'  path: {self.config.get_path("cd", "edges")}\n' +
                    helper.df_tostring(self.edges))
