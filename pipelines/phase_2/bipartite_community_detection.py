import logging
import infomap
import networkx as nx
import pandas as pd
from pipelines.pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class BipartiteCommunityDetection(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'find_communities',
                'file_name': 'graph',
                'file_extension': 'gexf',
                'r_kwargs': {
                    'node_type': int
                }
            },
            {
                'stage_name': 'find_communities',
                'file_name': 'multiplex_graph',
                'file_extension': 'gexf',
                'r_kwargs': {
                    'node_type': int
                }
            }
        ]
        tasks = [self.__find_communities]
        super(BipartiteCommunityDetection, self).__init__('bipartite_community_detection', files, tasks, datasources)

    def __find_communities(self):
        if not self.datasources.files.exists(
                'bipartite_community_detection', 'find_communities', 'graph', 'gexf'):
            graph = self.datasources.files.read(
                'bipartite_graph', 'get_user_hashtag_graph', 'graph', 'gexf')
            graph = nx.convert_node_labels_to_integers(graph, label_attribute='name')

            im = infomap.Infomap('--two-level --silent')

            is_multiplex = True

            # add edges and weights to network
            if is_multiplex:
                node_layer_dict = nx.get_node_attributes(graph, 'bipartite')
                for e in graph.edges(data=True):
                    # from (layer, node) to (layer, node) weight
                    im.addMultilayerLink(node_layer_dict[e[0]], e[0],
                                         node_layer_dict[e[1]], e[1],
                                         e[2]['weight'])
            else:
                for e in graph.edges(data=True):
                    im.addLink(e[0], e[1], e[2]['weight'])

            im.run()

            c = pd.DataFrame([{'node': n.physicalId, 'community': n.moduleIndex()}
                              for n in im.iterLeafNodes()]).set_index('node')

            # remove nodes with degree less than 30
            low_degree_nodes = [n for n, deg in graph.degree() if deg < 30]
            c = c.loc[~c.index.isin(low_degree_nodes)]

            # remove communities with only users
            c['is_hashtag'] = pd.Series(nx.get_node_attributes(graph, 'bipartite')).astype('bool')
            c = c.groupby('community').filter(lambda x: x['is_hashtag'].any())

            # rename communities
            communities_dict = {x: i for i, x in enumerate(c['community'].unique())}
            c.community = c.community.map(communities_dict.get)

            # remove nodes from graph (lone nodes, nodes with less than 30 degree and communities with no hashtag)
            graph.remove_nodes_from(set(graph.nodes) - set(c.index.tolist()))

            # add community attribute to nodes
            nx.set_node_attributes(graph, name='community', values=c.to_dict('dict')['community'])

            if is_multiplex:
                self.datasources.files.write(
                    graph, 'bipartite_community_detection', 'find_communities', 'multiplex_graph', 'gexf')
            else:
                self.datasources.files.write(
                    graph, 'bipartite_community_detection', 'find_communities', 'graph', 'gexf')
