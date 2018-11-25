from ..config_base import ConfigBase


class Config(ConfigBase):
    def __init__(self, project_name, dataset_name, cd_config):
        super(Config, self).__init__(project_name, dataset_name, 'community_detection')

        self.cd_config = cd_config

        self.comparison = f'{self.base_dir["input"]}/comparison/{dataset_name}.csv'

        postfix_args = '-'.join(f'{arg_name}{arg_value}' for arg_name, arg_value in self.cd_config[1].items())
        self.postfix = f'__{self.cd_config[0]}{f"({postfix_args})" if postfix_args else ""}'
