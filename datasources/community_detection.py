import json
import os


class CommunityDetection:
    def __init__(self, input_path):
        self.input_events_path = os.path.join(input_path, 'community_detection/community_detection.json')

    def get_community_detection_settings(self):
        with open(self.input_events_path) as json_file:
            proxy_list = json.load(json_file)

        return proxy_list
