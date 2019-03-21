import json
import os


class Node2Vec:
    def __init__(self, input_path):
        self.input_path = os.path.join(input_path, 'node2vec/node2vec.json')

    def get_node2vec_settings(self):
        with open(self.input_path) as json_file:
            node2vec_settings = json.load(json_file)

        return node2vec_settings
