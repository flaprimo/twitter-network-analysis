import os


class Config:
    def __init__(self, project_name, data_filename, cd_config):
        self.data_filename = data_filename

        self.cd_config = cd_config

        self.base_dir = {
            'input': 'data',
            'output': f'output/{project_name}/community_detection'
        }

        self.data_path = f'{self.base_dir["input"]}/{data_filename}.csv'

        self.comparison = f'{self.base_dir["input"]}/comparison/{data_filename}.csv'

        postfix_args = '-'.join(f'{arg_name}{arg_value}' for arg_name, arg_value in self.cd_config[1].items())
        self.postfix = f'__{self.cd_config[0]}{f"({postfix_args})" if postfix_args else ""}'

    def get_path(self, stage, file_name, file_type='csv'):
        directory = f'{self.base_dir["output"]}/{stage}'

        if not os.path.exists(directory):
            os.makedirs(directory)

        return f'{directory}/{self.data_filename}__{file_name}{self.postfix}.{file_type}'
