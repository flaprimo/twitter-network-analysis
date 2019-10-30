import json
import os


class CommunityDetection:
    def __init__(self, input_path):
        self.input_path = os.path.join(input_path, 'community_detection.json')

    def get_config(self):
        with open(self.input_path, 'r') as json_file:
            community_detection_settings = json.load(json_file)

        return community_detection_settings
