import os


class Config:
    def __init__(self, data_filename, cd_config):
        self.data_filename = data_filename

        self.cd_config = cd_config

        self.base_dir = {
            'input': 'data',
            'output': 'output/cd'
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
                'Username': str,
                'Community': 'uint16'
            },
            'csv_edges': {
                'Source': 'uint32',
                'Target': 'uint32',
                'Weight': 'uint8'
            }
        }

        self.data_path = f'{self.base_dir["input"]}/{data_filename}.csv'

        self.comparison = f'{self.base_dir["input"]}/comparison/{data_filename}.csv'

        postfix_args = '-'.join(f'{arg_name}{arg_value}' for arg_name, arg_value in self.cd_config[1].items())
        self.postfix = f'__{self.cd_config[0]}{f"({postfix_args})" if postfix_args else ""}'

    def get_path(self, stage, file_name, file_type='csv', has_postfix=True):
        directory = f'{self.base_dir["output"]}/{stage}'

        if not os.path.exists(directory):
            os.makedirs(directory)

        return f'{directory}/{self.data_filename}__{file_name}{self.postfix if has_postfix else ""}.{file_type}'
