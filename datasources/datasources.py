from .database.database import Database
from .files import Files
from .contexts import Contexts
from .community_detection import CommunityDetection
from .node2vec import Node2Vec


class Datasources:
    def __init__(self, input_path, output_path):
        self.files = Files(output_path)
        self.database = Database(output_path)
        self.contexts = Contexts(input_path)
        self.community_detection = CommunityDetection(input_path)
        self.node2vec = Node2Vec(input_path)
