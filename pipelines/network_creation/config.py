import os


class Config:
    def __init__(self, project_name, data_filename):
        self.data_filename = data_filename
        self.postfix = ''

        self.base_dir = {
            'input': '',
            'output': f'output/{project_name}/network_creation'
        }

    def get_path(self, stage, file_name, file_type='csv'):
        directory = f'{self.base_dir["output"]}/{stage}'

        if not os.path.exists(directory):
            os.makedirs(directory)

        return f'{directory}/{self.data_filename}__{file_name}{self.postfix}.{file_type}'
