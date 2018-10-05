import pandas as pd
import networkx as nx
import logging
import helper
import infomap
from .pipeline_io import PipelineIO

logger = logging.getLogger(__name__)


class CommunityDetection:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['edges', 'nodes'], stage_input, stage_input_format)
        self.output_prefix = 'cd'
        self.output_format = {
            'edges': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'edges'),
                'r_kwargs': {'dtype': self.config.data_type['csv_edges']},
                'w_kwargs': {'index': False}
            },
            'nodes': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'nodes'),
                'r_kwargs': {'dtype': self.config.data_type['csv_nodes'],
                             'index_col': 0},
                'w_kwargs': {}
            },
            'graph': {
                'type': 'networkx',
                'path': self.config.get_path(self.output_prefix, 'graph', 'gexf'),
                'r_kwargs': {'node_type': int},
                'w_kwargs': {}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['graph'] = self.__get_graph(self.input['edges'], self.input['nodes'])
            self.output['graph'] = self.__find_communities(self.output['graph'])
            self.output['nodes'] = self.__add_community_to_nodes(self.output['graph'], self.input['nodes'])
            self.output['graph'], self.output['nodes'], self.output['edges'] = \
                self.__remove_lone_nodes(self.output['graph'], self.input['nodes'], self.input['edges'])

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.data_filename}')

        return self.output, self.output_format

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
        im = infomap.Infomap('--two-level --directed --silent')
        im_network = im.network()

        for e in graph.edges(data=True):
            im_network.addLink(e[0], e[1], e[2]['Weight'])

        im.run()

        communities = {node.physicalId: node.moduleIndex() for node in im.iterLeafNodes()}
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
