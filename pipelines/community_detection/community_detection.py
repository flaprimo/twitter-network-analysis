import pandas as pd
import networkx as nx
import logging
import helper
from datasources import PipelineIO
from sqlalchemy.exc import IntegrityError
from datasources.database.database import db
from datasources.database.model import User

logger = logging.getLogger(__name__)


class CommunityDetection:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['edges', 'nodes', 'graph'], stage_input, stage_input_format)
        self.output_prefix = 'cd'
        self.output_format = {
            'edges': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'edges'),
                'r_kwargs': {
                    'dtype': {
                        'source_id': 'uint32',
                        'target_id': 'uint32',
                        'weight': 'uint16'
                    }
                },
                'w_kwargs': {'index': False}
            },
            'nodes': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'nodes'),
                'r_kwargs': {
                    'dtype': {
                        'community': 'uint16',
                        'user_id': 'uint32',
                        'user_name': str
                    },
                },
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
                'r_kwargs': {
                    'dtype': {
                        'community': 'uint16',
                        'user_id': 'uint32'
                    },
                },
                'w_kwargs': {'index': False}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.skip_output_check or not self.output:
            self.output['communities'] = self.__find_communities(self.input['graph'], self.config.cd_config)
            self.output['graph'], self.output['nodes'], self.output['edges'] = \
                self.__remove_lone_nodes(self.output['communities'], self.input['graph'],
                                         self.input['nodes'], self.input['edges'])
            self.output['nodes'] = self.__add_community_to_nodes(self.output['communities'], self.input['nodes'])
            self.output['graph'] = self.__add_community_to_graph(self.output['communities'], self.output['graph'])

            if self.config.save_db_output:
                self.__persist_users(self.output['nodes'])

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __find_communities(graph, cd_config):
        def demon_alg(g, epsilon, min_community_size):
            import demon as d

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
            import infomap

            im = infomap.Infomap('--two-level --directed --silent')
            im_network = im.network()

            # add edges and weights to network
            for e in g.edges(data=True):
                im_network.addLink(e[0], e[1], e[2]['weight'])

            im.run()

            return pd.DataFrame([{'user_id': n.physicalId, 'community': n.moduleIndex()} for n in im.iterLeafNodes()])

        def girvan_newman_alg(g):
            from networkx.algorithms.community import girvan_newman

            results = girvan_newman(g)

            c = []
            for c_name, c_nodes in enumerate(results):
                for n in c_nodes:
                    c.append({'user_id': n, 'community': c_name})

            return pd.DataFrame(c)

        cd_algs = {
            'demon': demon_alg,
            'infomap': infomap_alg,
            'girvan_newman': girvan_newman_alg
        }

        try:
            alg = cd_algs[cd_config[0]]
        except KeyError:
            raise KeyError('community algorithm detection name is wrong, check the configuration')

        logger.info(f'find communities with algorithm: {cd_config[0]}')

        communities = alg(graph, **cd_config[1])

        # check if empty (no communities have been found)
        # if empty assign all nodes to the same community
        if communities.empty:
            communities = pd.DataFrame({'user_id': graph.nodes, 'community': 0})

        logger.debug(f'found {communities.community.nunique()} communities\n' +
                     helper.df_tostring(communities, 5))

        return communities

    @staticmethod
    def __remove_lone_nodes(communities, graph, nodes, edges):
        nodes_to_keep = set([n['user_id'] for n in communities.to_dict('records')])
        nodes_total = set(nodes.index.values)
        lone_nodes = list(nodes_total - nodes_to_keep)

        nodes = nodes.drop(nodes.index[lone_nodes])
        edges = edges[~(edges.source_id.isin(lone_nodes) | edges.target_id.isin(lone_nodes))]
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

    @staticmethod
    def __persist_users(nodes):
        logger.info('persist graph')
        user_name_list = nodes['user_name'].drop_duplicates().tolist()

        try:
            with db.session_scope() as session:
                # filter users already present
                users_to_filter = session.query(User.user_name)\
                    .filter(User.user_name.in_(user_name_list)).all()
                users_to_filter = [u[0] for u in users_to_filter]
                user_name_list = list(filter(lambda x: x not in users_to_filter, user_name_list))

                # persist new users
                user_entities = [User(user_name=u) for u in user_name_list]
                session.add_all(user_entities)
            logger.debug('users successfully persisted')
        except IntegrityError:
            logger.debug('user already exists or constraint is violated and could not be added')
