import pandas as pd
import networkx as nx
import demon as d
import logging
import helper
import infomap
from pipelines.pipeline_io import PipelineIO

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
                'r_kwargs': {'dtype': self.config.data_type['csv_nodes']},
                'w_kwargs': {'index': False}
            },
            'graph': {
                'type': 'networkx',
                'path': self.config.get_path(self.output_prefix, 'graph', 'gexf'),
                'r_kwargs': {'node_type': int},
                'w_kwargs': {}
            },
            'communities': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'communities'),
                'r_kwargs': {'dtype': self.config.data_type['csv_nodes']},
                'w_kwargs': {'index': False}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['graph'] = self.__get_graph(self.input['edges'], self.input['nodes'])
            self.output['communities'] = self.__find_communities(self.output['graph'], self.config.cd_config)
            self.output['graph'], self.output['nodes'], self.output['edges'] = \
                self.__remove_lone_nodes(self.output['communities'], self.output['graph'],
                                         self.input['nodes'], self.input['edges'])
            self.output['nodes'] = self.__add_community_to_nodes(self.output['communities'], self.input['nodes'])
            self.output['graph'] = self.__add_community_to_graph(self.output['communities'], self.output['graph'])

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.data_filename}')

        return self.output, self.output_format

    @staticmethod
    def __get_graph(edges, nodes):
        graph = nx.from_pandas_edgelist(edges,
                                        source='Source', target='Target', edge_attr=['Weight'],
                                        create_using=nx.DiGraph())
        nx.set_node_attributes(graph, pd.Series(nodes.user_name).to_dict(), 'user_name')

        logger.info('get graph')
        logger.debug(helper.graph_tostring(graph, 3, 3))

        return graph

    @staticmethod
    def __find_communities(graph, cd_config):
        def demon_alg(g, epsilon, min_community_size):
            dm = d.Demon(graph=g,
                         epsilon=epsilon,
                         min_community_size=min_community_size)
            results = dm.execute()

            # remove 'communities' attribute
            for n in g.nodes(data=True):
                n[1].pop('communities', None)

            c = []
            for c_name, c_nodes in enumerate(results):
                for n in c_nodes:
                    c.append({'user_id': n, 'community': c_name})

            return pd.DataFrame(c)

        def infomap_alg(g):
            im = infomap.Infomap('--two-level --directed --silent')
            im_network = im.network()

            for e in g.edges(data=True):
                im_network.addLink(e[0], e[1], e[2]['Weight'])

            im.run()

            return pd.DataFrame([{'user_id': n.physicalId, 'community': n.moduleIndex()} for n in im.iterLeafNodes()])

        cd_algs = {
            'demon': demon_alg,
            'infomap': infomap_alg
        }

        try:
            alg = cd_algs[cd_config[0]]
        except KeyError:
            raise KeyError('community algorithm detection name is wrong, check the configuration')

        logger.info(f'find communities with algorithm: {cd_config[0]}')

        communities = alg(graph, **cd_config[1])

        # check if empty (no communities have been found)
        if communities.empty:
            communities = pd.DataFrame({'user_id': graph.nodes})
            communities['community'] = communities.user_id
            print('HEY')
            print(helper.df_tostring(communities))

        # try:
        #     len_communities = communities.community.nunique()
        # except AttributeError:
        #     print('HEY')
        #     len_communities = 0
        logger.debug(f'found {communities.community.nunique()} communities\n' +
                     helper.df_tostring(communities, 5))

        return communities

    @staticmethod
    def __remove_lone_nodes(communities, graph, nodes, edges):
        nodes_to_keep = set([n['user_id'] for n in communities.to_dict('records')])
        nodes_total = set(nodes.index.values)
        lone_nodes = list(nodes_total - nodes_to_keep)

        nodes = nodes.drop(nodes.index[lone_nodes])
        edges = edges[~(edges.Source.isin(lone_nodes) | edges.Target.isin(lone_nodes))]
        graph.remove_nodes_from(lone_nodes)

        logger.info('remove lone nodes')
        logger.debug(f'removed {len(nodes_total)}-{len(nodes_to_keep)}={len(lone_nodes)} nodes: {lone_nodes}' +
                     helper.df_tostring(nodes, 5) +
                     helper.graph_tostring(graph, 3, 3))

        return graph, nodes, edges

    @staticmethod
    def __add_community_to_nodes(communities, nodes):
        nodes = pd.merge(communities, nodes, left_on='user_id', right_index=True) \
            .sort_values(by=['user_id', 'community'])

        logger.info('add communities to nodes')
        logger.debug(helper.df_tostring(nodes, 5))

        return nodes

    @staticmethod
    def __add_community_to_graph(communities, graph):
        c_records = communities.to_dict('records')
        c_range = set([n['community'] for n in c_records])

        for c_name in c_range:
            comms = {n['user_id']: True for n in c_records if n['community'] == c_name}
            nx.set_node_attributes(graph, values=comms, name=f'C_{c_name}')

        logger.info('add communities to graph')
        logger.debug(helper.graph_tostring(graph, 3, 3))

        return graph
