import logging
import networkx as nx
import pandas as pd
import pquality.PartitionQuality as Pq
from sqlalchemy.exc import IntegrityError
from datasources.database.model import Graph, Partition, Event, Community, User, UserCommunity
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class CommunityDetectionMetrics(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name':  'pquality',
                'file_name':  'pquality',
                'file_extension':  'csv',
                'r_kwargs':  {
                    'dtype': {
                        'index': str,
                        'min': 'float32',
                        'max': 'float32',
                        'avg': 'float32',
                        'std': 'float32'
                    },
                    'index_col': 'index'
                }
            },
            {
                'stage_name':  'partition_summary',
                'file_name':  'partition_summary',
                'file_extension':  'csv',
                'r_kwargs':  {
                    'dtype': {
                        'community': 'uint16',
                        'no_nodes': 'uint16',
                        'no_edges': 'uint16',
                        'avg_degree': 'float32',
                        'avg_weighted_degree': 'float32',
                        'density': 'float32',
                        'connected': bool,
                        'strongly_conn_components': 'uint16',
                        'avg_clustering': 'float32',
                        'assortativity': 'float32'
                    },
                    'index_col': 'community'
                }
            },
            {
                'stage_name':  'node_metrics',
                'file_name':  'nodes',
                'file_extension':  'csv',
                'r_kwargs':  {
                    'dtype': {
                        'community': 'uint16',
                        'user_id': 'uint32',
                        'user_name': str,
                        'indegree': 'float32',
                        'indegree_centrality': 'float32',
                        'hindex': 'uint16'
                    }
                },
                'w_kwargs':  {
                    'index': False
                }
            }
        ]
        tasks = [self.__pquality, self.__partition_summary, self.__node_metrics]
        super(CommunityDetectionMetrics, self)\
            .__init__('community_detection_metrics', files, tasks, datasources, file_prefix)

    def __pquality(self):
        if not self.datasources.files.exists(
                'community_detection_metrics', 'pquality', 'pquality', 'csv', self.context_name):
            graph = self.datasources.files.read(
                'community_detection', 'add_communities_to_graph', 'graph', 'gexf', self.context_name)
            nodes = self.datasources.files.read(
                'community_detection', 'add_communities_to_nodes', 'nodes', 'csv', self.context_name)

            communities = [graph.subgraph(tuple(v.values))
                           for k, v in nodes.set_index('user_id').groupby('community').groups.items()]

            pqualities = [
                ('internal_density', Pq.internal_edge_density, 1, []),
                ('edges_inside', Pq.edges_inside, 1, []),
                ('normalized_cut', Pq.normalized_cut, 2, []),
                ('avg_degree', Pq.average_internal_degree, 1, []),
                ('fomd', Pq.fraction_over_median_degree, 1, []),
                ('expansion', Pq.expansion, 2, []),
                ('cut_ratio', Pq.cut_ratio, 2, []),
                ('conductance', Pq.conductance, 2, []),
                ('max_odf', Pq.max_odf, 2, []),
                ('avg_odf', Pq.avg_odf, 2, []),
                ('flake_odf', Pq.flake_odf, 2, [])
            ]

            m = []
            for pq_name, pq_func, pq_arg_len, pq_values in pqualities:
                for c in communities:
                    pq_values.append(pq_func(graph, c) if pq_arg_len == 2 else pq_func(c))
                m.append([pq_name, min(pq_values), max(pq_values), pd.np.mean(pq_values), pd.np.std(pq_values)])

            pquality_df = pd.DataFrame(m, columns=['index', 'min', 'max', 'avg', 'std']).set_index('index')

            partition_record = {r['index']: r['avg']
                                for r in pquality_df['avg'].reset_index().to_dict('records')}

            try:
                with self.datasources.database.session_scope() as session:
                    graph_entity = session.query(Graph).join(Graph.event) \
                        .filter(Event.name == self.context_name).first()
                    partition_entity = Partition(**partition_record, graph=graph_entity)
                    session.add(partition_entity)
                logger.debug('partition successfully persisted')
            except IntegrityError:
                logger.debug('partition already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                pquality_df, 'community_detection_metrics', 'pquality', 'pquality', 'csv', self.context_name)

    def __partition_summary(self):
        def graph_summary(graph):
            # NaN assortatitvity: https://groups.google.com/forum/#!topic/networkx-discuss/o2zl40LMmqM
            def assortativity(g):
                try:
                    return nx.degree_assortativity_coefficient(g)
                except ValueError:
                    return None

            summary_df = pd.DataFrame(data={
                'no_nodes': graph.number_of_nodes(),
                'no_edges': graph.number_of_edges(),
                'avg_degree': sum([x[1] for x in graph.degree()]) / graph.number_of_nodes(),
                'avg_weighted_degree': sum([x[1] for x in graph.degree(weight='weight')]) / graph.number_of_nodes(),
                'density': nx.density(graph),
                'connected': nx.is_weakly_connected(graph),
                'strongly_conn_components': nx.number_strongly_connected_components(graph),
                'avg_clustering': nx.average_clustering(graph),
                'assortativity': assortativity(graph)
            }, index=[0]).round(4)

            return summary_df
        
        if not self.datasources.files.exists(
                'community_detection_metrics', 'partition_summary', 'partition_summary', 'csv', self.context_name):
            graph = self.datasources.files.read(
                'community_detection', 'add_communities_to_graph', 'graph', 'gexf', self.context_name)
            nodes = self.datasources.files.read(
                'community_detection', 'add_communities_to_nodes', 'nodes', 'csv', self.context_name)

            communities = [(k, graph.subgraph(tuple(v.values)))
                           for k, v in nodes.set_index('user_id').groupby('community').groups.items()]

            c_summary_list = []
            for c_name, c_graph in communities:
                c_summary_df = graph_summary(c_graph)
                c_summary_df['community'] = c_name
                c_summary_list.append(c_summary_df)

            partition_summary_df = pd.concat(c_summary_list).set_index('community')

            communities = [{'name': c} for c in partition_summary_df.index.tolist()]

            try:
                with self.datasources.database.session_scope() as session:
                    partition_entity = session.query(Partition).join(Partition.graph).join(Graph.event) \
                        .filter(Event.name == self.context_name).first()

                    community_entities = [Community(**c, partition=partition_entity)
                                          for c in communities]
                    session.add_all(community_entities)
                logger.debug('communities successfully persisted')
            except IntegrityError:
                logger.debug('community already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                partition_summary_df, 'community_detection_metrics', 'partition_summary', 'partition_summary',
                'csv', self.context_name)

    def __node_metrics(self):
        if not self.datasources.files.exists(
                'community_detection_metrics', 'node_metrics', 'nodes', 'csv', self.context_name):
            graph = self.datasources.files.read(
                'community_detection', 'add_communities_to_graph', 'graph', 'gexf', self.context_name)
            nodes = self.datasources.files.read(
                'community_detection', 'add_communities_to_nodes', 'nodes', 'csv', self.context_name)

            def indegree(g):
                return [{'user_id': n, 'indegree': g.in_degree(n)} for n in g.nodes]

            def indegree_centrality(g):
                return [{'user_id': n, 'indegree_centrality': ic} for n, ic in nx.in_degree_centrality(g).items()]

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
                    edges = [e[2]['weight'] for e in g.in_edges(n, data=True)]
                    hindex_list.append({'user_id': n, 'hindex': alg_hindex(edges)})

                return hindex_list

            communities = [(k, graph.subgraph(tuple(v.values)))
                           for k, v in nodes.set_index('user_id').groupby('community').groups.items()]

            nm_metrics = [indegree, indegree_centrality, hindex]

            for nm_func in nm_metrics:
                results = []
                for c_name, c_graph in communities:
                    results.extend([{**n, 'community': c_name} for n in nm_func(c_graph)])

                nodes = pd.merge(nodes, pd.DataFrame(results),
                                 left_on=['user_id', 'community'], right_on=['user_id', 'community'])

            try:
                with self.datasources.database.session_scope() as session:
                    # get all commmunities for current dataset partition
                    community_entities = session.query(Community) \
                        .join(Community.partition).join(Partition.graph).join(Graph.event) \
                        .filter(Event.name == self.context_name).all()

                    # get all users for current dataset
                    user_entities = session.query(User) \
                        .filter(User.user_name.in_(nodes['user_name'].drop_duplicates().tolist())).all()

                    usercommunity_entities = []
                    for u in nodes.to_dict('records'):
                        # get user and community entities and usercommunity info
                        community_entity = next(filter(lambda x: x.name == u['community'], community_entities), None)
                        user_entity = next(filter(lambda x: x.user_name == u['user_name'], user_entities), None)

                        # create usercommunity entity
                        usercommunity_entity = UserCommunity(indegree=u['indegree'],
                                                             indegree_centrality=u['indegree_centrality'],
                                                             hindex=u['hindex'],
                                                             user=user_entity, community=community_entity)
                        usercommunity_entities.append(usercommunity_entity)
                    session.add_all(usercommunity_entities)
            except IntegrityError:
                logger.debug('usercommunity already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                nodes, 'community_detection_metrics', 'node_metrics', 'nodes', 'csv', self.context_name)
