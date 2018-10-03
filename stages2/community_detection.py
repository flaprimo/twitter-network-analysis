import pandas as pd
import networkx as nx
import logging
import helper
import infomap

logger = logging.getLogger(__name__)


class CommunityDetection:
    def __init__(self, config, stage_input=None):
        self.config = config
        self.prev_stage_prefix = 'pp'
        self.stage_prefix = 'cd'
        self.input = self.__load_input(stage_input)
        self.output = self.__load_output()
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['graph'] = self.__get_graph(self.input['edges'], self.input['nodes'])
            self.output['graph'] = self.__find_communities(self.output['graph'])
            self.output['nodes'] = self.__add_community_to_nodes(self.output['graph'], self.input['nodes'])
            self.output['graph'], self.output['nodes'], self.output['edges'] = \
                self.__remove_lone_nodes(self.output['graph'], self.input['nodes'], self.input['edges'])
            self.__save_output()

        return self.output

    def __load_input(self, stage_input):
        logger.info('load input')
        if helper.check_input(['edges', 'nodes'], stage_input):
            logger.debug(f'input present')
            return stage_input
        else:
            logger.debug(f'input not present, loading input')
            return {
                'edges': pd.read_csv(self.config.get_path(self.prev_stage_prefix, 'edges'),
                                     dtype=self.config.data_type['csv_edges']),
                'nodes': pd.read_csv(self.config.get_path(self.prev_stage_prefix, 'nodes'),
                                     dtype=self.config.data_type['csv_nodes'], index_col=0),
            }

    def __load_output(self):
        logger.info('load output')
        try:
            output = {
                'graph': nx.read_gexf(self.config.get_path(self.stage_prefix, 'graph', 'gexf')),
                'nodes': pd.read_csv(self.config.get_path(self.stage_prefix, 'nodes'),
                                     dtype=self.config.data_type['csv_nodes']),
                'edges': pd.read_csv(self.config.get_path(self.stage_prefix, 'edges'),
                                     dtype=self.config.data_type['csv_nodes'])
            }
            logger.debug(f'output present, not executing stage')

            return output
        except IOError as e:
            logger.debug(f'output not present, executing stage: {e}')

            return {}

    def __save_output(self):
        graph_path = self.config.get_path(self.stage_prefix, 'graph', 'gexf')
        nodes_path = self.config.get_path(self.stage_prefix, 'nodes')
        edges_path = self.config.get_path(self.stage_prefix, 'edges')

        nx.write_gexf(self.output['graph'], graph_path)
        self.output['nodes'].to_csv(nodes_path)
        self.output['edges'].to_csv(edges_path, index=False)

        logger.info('save output')
        logger.debug(f'nodes file path: {nodes_path}\n' +
                     helper.df_tostring(self.output['nodes'], 5) +
                     f'edges file path: {nodes_path}\n' +
                     helper.df_tostring(self.output['edges'], 5) +
                     f'graph file path: {graph_path}\n' +
                     helper.graph_tostring(self.output['graph'], 3, 3))

    @staticmethod
    def __get_graph(edges, nodes):
        graph = nx.from_pandas_edgelist(edges,
                                        source='Source', target='Target', edge_attr=['Weight'],
                                        create_using=nx.DiGraph())
        nx.set_node_attributes(graph, pd.Series(nodes.Username).to_dict(), 'Username')

        logger.info('get graph')
        logger.debug(helper.graph_tostring(graph, 3, 3))

        return graph

    @staticmethod
    def __find_communities(graph):
        im = infomap.Infomap('--two-level --directed')
        im_network = im.network()

        for e in graph.edges(data=True):
            im_network.addLink(e[0], e[1], e[2]['Weight'])

        im.run()

        communities = {}
        for node in im.iterLeafNodes():
            communities[node.physicalId] = node.moduleIndex()

        nx.set_node_attributes(graph, values=communities, name='Community')

        logger.info('find communities')
        logger.debug(f'found {im.numTopModules()} top modules with codelength: {im.codelength()}' +
                     helper.graph_tostring(graph, 3, 3))

        return graph

    @staticmethod
    def __add_community_to_nodes(graph, nodes):
        nodes['Community'] = pd.Series(nx.get_node_attributes(graph, 'Community'))

        logger.info('add communities to nodes')
        logger.debug(helper.df_tostring(nodes, 5))

        return nodes

    @staticmethod
    def __remove_lone_nodes(graph, nodes, edges):
        lone_nodes = list(nodes[nodes.isnull().any(axis=1)].index)

        nodes = nodes.dropna().copy()
        nodes.Community = pd.to_numeric(nodes.Community, downcast='unsigned')
        edges = edges[~(edges.Source.isin(lone_nodes) | edges.Target.isin(lone_nodes))]
        graph.remove_nodes_from(lone_nodes)

        logger.info('remove lone nodes')
        logger.debug(f'removed {len(lone_nodes)} nodes: {lone_nodes}')
        logger.debug(helper.df_tostring(nodes, 5))

        return graph, nodes, edges
