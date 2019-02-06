import logging
import networkx as nx
import pandas as pd
from sqlalchemy.exc import IntegrityError
from datasources.database.model import User
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class CommunityDetection(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name':  'find_communities',
                'file_name':  'communities',
                'file_extension':  'csv',
                'r_kwargs':  {
                    'dtype': {
                        'community': 'uint16',
                        'user_id': 'uint32'
                    }
                },
                'w_kwargs':  {
                    'index': False
                }
            },
            {
                'stage_name':  'add_communities_to_nodes',
                'file_name':  'nodes',
                'file_extension':  'csv',
                'r_kwargs':  {
                    'dtype': {
                        'community': 'uint16',
                        'user_id': 'uint32',
                        'user_name': str
                    }
                },
                'w_kwargs':  {
                    'index': False
                }
            },
            {
                'stage_name':  'add_communities_to_graph',
                'file_name':  'graph',
                'file_extension':  'gexf',
                'r_kwargs':  {
                    'node_type': int
                }
            },
            {
                'stage_name':  'remove_lone_nodes_from_edges',
                'file_name':  'edges',
                'file_extension':  'csv',
                'r_kwargs':  {
                    'dtype': {
                        'source_id': 'uint32',
                        'target_id': 'uint32',
                        'weight': 'uint16'
                    }
                },
                'w_kwargs':  {
                    'index': False
                }
            }
        ]
        tasks = [self.__find_communities, self.__add_communities_to_nodes, self.__add_communities_to_graph,
                 self.__remove_lone_nodes_from_edges]
        super(CommunityDetection, self).__init__('community_detection', files, tasks, datasources, file_prefix)

    def __find_communities(self):
        if not self.datasources.files.exists(
                'community_detection', 'find_communities', 'communities', 'csv', self.context_name):
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

                c = pd.DataFrame([{'user_id': n.physicalId, 'community': n.moduleIndex()}
                                  for n in im.iterLeafNodes()])
                c = c.groupby('community').filter(lambda x: len(x) > 3)

                # renumber communities
                communities_dict = {x: i for i, x in enumerate(c['community'].unique())}
                c.community = c.community.map(communities_dict.get)

                return c

            graph = self.datasources.files.read(
                'network_creation', 'create_graph', 'graph', 'gexf', self.context_name)
            cd_config = self.datasources.community_detection.get_community_detection_settings()

            cd_algs = {
                'demon': demon_alg,
                'infomap': infomap_alg
            }

            try:
                alg = cd_algs[cd_config['name']]
            except KeyError:
                raise KeyError('community algorithm detection name is wrong, check the configuration')

            logger.info(f'find communities with algorithm: {cd_config["name"]}')

            communities = alg(graph, **cd_config['kwargs'])

            # if empty (no communities have been found), assign all nodes to the same community
            if communities.empty:
                communities = pd.DataFrame({'user_id': graph.nodes, 'community': 0})

            self.datasources.files.write(
                communities, 'community_detection', 'find_communities', 'communities', 'csv', self.context_name)

    def __add_communities_to_nodes(self):
        if not self.datasources.files.exists(
                'community_detection', 'add_communities_to_nodes', 'nodes', 'csv', self.context_name):
            communities = self.datasources.files.read(
                'community_detection', 'find_communities', 'communities', 'csv', self.context_name)
            nodes = self.datasources.files.read(
                'network_creation', 'create_nodes', 'nodes', 'csv', self.context_name)

            # also drops lone nodes
            nodes = pd.merge(communities, nodes, left_on='user_id', right_index=True) \
                .sort_values(by=['user_id', 'community'])
            nodes = nodes[['user_id', 'user_name', 'community']]

            user_name_list = nodes['user_name'].drop_duplicates().tolist()
            try:
                with self.datasources.database.session_scope() as session:
                    # filter users already present
                    users_to_filter = session.query(User.user_name) \
                        .filter(User.user_name.in_(user_name_list)).all()
                    users_to_filter = [u[0] for u in users_to_filter]
                    user_name_list = list(filter(lambda x: x not in users_to_filter, user_name_list))

                    # persist new users
                    user_entities = [User(user_name=u) for u in user_name_list]
                    session.add_all(user_entities)
                logger.debug('users successfully persisted')
            except IntegrityError:
                logger.debug('users already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                nodes, 'community_detection', 'add_communities_to_nodes', 'nodes', 'csv', self.context_name)

    def __add_communities_to_graph(self):
        if not self.datasources.files.exists(
                'community_detection', 'add_communities_to_graph', 'graph', 'gexf', self.context_name):
            graph = self.datasources.files.read(
                'network_creation', 'create_graph', 'graph', 'gexf', self.context_name)
            nodes = self.datasources.files.read(
                'community_detection', 'add_communities_to_nodes', 'nodes', 'csv', self.context_name)

            # remove lone nodes
            lone_nodes = graph.nodes - nodes['user_id'].tolist()
            graph.remove_nodes_from(lone_nodes)

            # community node dictionary (keep only True community attribute)
            communities = pd.get_dummies(nodes.set_index('user_id')['community'], prefix='C')\
                .astype(bool).to_dict('index')
            communities = {n_name: {c_name: c_value} for n_name, attributes in communities.items()
                           for c_name, c_value in attributes.items() if c_value}
            nx.set_node_attributes(graph, communities)

            self.datasources.files.write(
                graph, 'community_detection', 'add_communities_to_graph', 'graph', 'gexf', self.context_name)

    def __remove_lone_nodes_from_edges(self):
        if not self.datasources.files.exists(
                'community_detection', 'remove_lone_nodes_from_edges', 'edges', 'csv', self.context_name):
            nodes = self.datasources.files.read(
                'community_detection', 'add_communities_to_nodes', 'nodes', 'csv', self.context_name)
            edges = self.datasources.files.read(
                'network_creation', 'create_edges', 'edges', 'csv', self.context_name)

            edges = edges[edges.source_id.isin(nodes['user_id']) | edges.target_id.isin(nodes['user_id'])]

            self.datasources.files.write(
                edges, 'community_detection', 'remove_lone_nodes_from_edges', 'edges', 'csv', self.context_name)
