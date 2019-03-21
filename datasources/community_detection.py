import json
import os


class CommunityDetection:
    def __init__(self, input_path):
        self.input_path = os.path.join(input_path, 'community_detection/community_detection.json')

    def get_community_detection_settings(self):
        with open(self.input_path) as json_file:
            community_detection_settings = json.load(json_file)

        return community_detection_settings
