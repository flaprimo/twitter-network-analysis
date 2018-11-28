import os


class ConfigBase:
    def __init__(self, project_name, dataset_name, stage_name):
        self.dataset_name = dataset_name
        self.stage_name = stage_name

        self.base_dir = {
            'input': 'input',
            'output': f'output/{project_name}/{stage_name}'
        }

        self.postfix = ''

        self.check_output = True

    def get_path(self, stage, file_name, file_type='csv'):
        directory = f'{self.base_dir["output"]}/{stage}'

        if not os.path.exists(directory):
            os.makedirs(directory)

        return f'{directory}/{self.dataset_name}__{file_name}{self.postfix}.{file_type}'
