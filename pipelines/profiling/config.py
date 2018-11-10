import os


class Config:
    def __init__(self, data_filename, cd_postfix):
        self.data_filename = data_filename
        self.postfix = cd_postfix

        self.base_dir = {
            'input': 'output/community_detection',
            'output': 'output/profiling'
        }

        self.input_path = f'{self.base_dir["input"]}/{data_filename}.csv'

        self.comparison = f'{self.base_dir["input"]}/comparison/{data_filename}.csv'

    def get_path(self, stage, file_name, file_type='csv'):
        directory = f'{self.base_dir["output"]}/{stage}'

        if not os.path.exists(directory):
            os.makedirs(directory)

        return f'{directory}/{self.data_filename}__{file_name}{self.postfix}.{file_type}'
