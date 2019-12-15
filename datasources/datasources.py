from .database.database import Database
from .files import Files
from .contexts import Contexts
from .community_detection import CommunityDetection
from .context_detection import ContextDetection
from .tw_api import TwApi


class Datasources:
    def __init__(self, input_path, output_path):
        self.files = Files(output_path)
        self.database = Database(output_path)
        self.contexts = Contexts(input_path)
        self.community_detection = CommunityDetection(input_path)
        self.context_detection = ContextDetection(input_path)
        self.tw_api = TwApi(input_path, output_path)
