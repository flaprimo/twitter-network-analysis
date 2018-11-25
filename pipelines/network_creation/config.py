from ..config_base import ConfigBase


class Config(ConfigBase):
    def __init__(self, project_name, dataset_name):
        super(Config, self).__init__(project_name, dataset_name, 'network_creation')
