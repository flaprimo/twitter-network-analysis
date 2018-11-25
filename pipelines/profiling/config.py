from ..config_base import ConfigBase


class Config(ConfigBase):
    def __init__(self, project_name, dataset_name, p_postfix):
        super(Config, self).__init__(project_name, dataset_name, 'profiling')
        self.postfix = p_postfix
