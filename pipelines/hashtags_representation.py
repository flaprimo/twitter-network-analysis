import logging
from node2vec import Node2Vec
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class HashtagsRepresentation(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_nodes_representation',
                'file_name': 'node2vec_nodes',
                'file_extension': 'embedding'
            },
            {
                'stage_name': 'get_nodes_representation',
                'file_name': 'node2vec_nodes',
                'file_extension': 'embedding_model'
            },
            {
                'stage_name': 'get_edges_representation',
                'file_name': 'node2vec_edges',
                'file_extension': 'embedding'
            }
        ]
        tasks = [self.__get_nodes_representation, self.__get_edges_representation]
        super(HashtagsRepresentation, self).__init__('hashtags_representation', files, tasks, datasources)

    def __get_nodes_representation(self):
        if not self.datasources.files.exists(
                'hashtags_representation', 'get_nodes_representation', 'node2vec_nodes', 'embedding') or \
           not self.datasources.files.exists(
               'hashtags_representation', 'get_nodes_representation', 'node2vec_nodes', 'embedding_model'):
            graph = self.datasources.files.read('hashtags_network', 'create_graph', 'graph', 'gexf')
            node2vec_config = self.datasources.node2vec.get_node2vec_settings()

            # remove not useful node attributes
            for n in graph.nodes(data=True):
                del n[1]['hashtag']

            # Precompute probabilities and generate walks
            node2vec = Node2Vec(graph, **node2vec_config['node2vec'],
                                workers=2, temp_folder=self.datasources.files.output_path)

            # Embed nodes
            model = node2vec.fit(**node2vec_config['fit'])

            # Look for most similar nodes
            # model.wv.most_similar('2')  # Output node names are always strings

            # Save embeddings for later use
            self.datasources.files.write(
                model, 'hashtags_representation', 'get_nodes_representation', 'node2vec_nodes', 'embedding')
            self.datasources.files.write(
                model, 'hashtags_representation', 'get_nodes_representation', 'node2vec_nodes', 'embedding_model')

    def __get_edges_representation(self):
        if not self.datasources.files.exists(
                'hashtags_representation', 'get_edges_representation', 'node2vec_edges', 'embedding'):
            model = self.datasources.files.read(
                'hashtags_representation', 'get_nodes_representation', 'node2vec_nodes', 'embedding_model')

            # Embed edges using Hadamard method
            from node2vec.edges import HadamardEmbedder

            edges_embs = HadamardEmbedder(keyed_vectors=model.wv)

            # Look for embeddings on the fly - here we pass normal tuples
            # print(edges_embs[('1', '2')])
            ''' OUTPUT
            array([ 5.75068220e-03, -1.10937878e-02,  3.76693785e-01,  2.69105062e-02,
                   ... ... ....
                   ..................................................................],
                  dtype=float32)
            '''

            # Get all edges in a separate KeyedVectors instance - use with caution could be huge for big networks
            edges_kv = edges_embs.as_keyed_vectors()

            # Look for most similar edges - this time tuples must be sorted and as str
            # edges_kv.most_similar(str(('1', '2')))

            self.datasources.files.write(
                edges_kv, 'hashtags_representation', 'get_edges_representation', 'node2vec_edges', 'embedding')
