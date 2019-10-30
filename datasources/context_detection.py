import json
import os


class ContextDetection:
    def __init__(self, input_path):
        self.input_path = os.path.join(input_path, 'context_detection.json')

    def get_config(self):
        with open(self.input_path, 'r') as json_file:
            context_detection_settings = json.load(json_file)

        return context_detection_settings
