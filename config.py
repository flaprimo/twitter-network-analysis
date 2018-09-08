import os


class Config:
    def __init__(self, demon=None, keep_lone_nodes=False, data_filename='ll'):
        self.data_filename = data_filename

        self.base_dir = {
            'input': 'data',
            'output': 'output'
        }

        self.data_type = {
            'csv_data': {
                'cod': str,
                'user_from_name': str,
                'user_from_fav_count': 'uint8',
                'user_rt_fav_count': 'uint8',
                'user_to_name': str,
                'text': str,
                'weights': 'uint8'
            },
            'csv_nodes': {
                'Id': str
            },
            'csv_edges': {
                'Source': str,
                'Target': str,
                'Weight': 'uint8'
            }
        }

        self.data_path = f'{self.base_dir["input"]}/{data_filename}.csv'

        self.keep_lone_nodes = keep_lone_nodes

        self.comparison = f'{self.base_dir["input"]}/comparison/{data_filename}.csv'

        if demon:
            self.demon = demon
            self.postfix = f'_e{self.demon["epsilon"]}_mcs{self.demon["min_community_size"]}'
        else:
            self.postfix = ''

    def get_path(self, stage, file_name, file_type='csv', has_postfix=True):
        directory = f'{self.base_dir["output"]}/{stage}'

        if not os.path.exists(directory):
            os.makedirs(directory)

        return f'{directory}/{self.data_filename}_{file_name}{self.postfix if has_postfix else ""}.{file_type}'
