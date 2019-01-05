import os


class ConfigBase:
    def __init__(self, project_name, dataset_name, stage_name,
                 skip_output_check=False, save_io_output=True, save_db_output=True):
        self.dataset_name = dataset_name
        self.stage_name = stage_name

        self.base_dir = {
            'input': 'input',
            'output': f'output/{project_name}/{stage_name}'
        }

        self.postfix = ''

        self.skip_output_check = skip_output_check
        self.save_io_output = save_io_output
        self.save_db_output = save_db_output

    def get_path(self, stage, file_name, file_type='csv'):
        directory = f'{self.base_dir["output"]}/{stage}'

        if not os.path.exists(directory):
            os.makedirs(directory)

        return f'{directory}/{self.dataset_name}__{file_name}{self.postfix}.{file_type}'
