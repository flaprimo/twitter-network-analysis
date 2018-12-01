import logging
import helper
from datasources import PipelineIO

logger = logging.getLogger(__name__)


class CreateNetwork:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['stream'], stage_input, stage_input_format)
        self.output_prefix = 'cn'
        self.output_format = {
            'network': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'network'),
                'r_kwargs': {
                    'dtype': {
                        'cod': str,
                        'user_from_name': str,
                        'user_from_fav_count': 'uint16',
                        'user_rt_fav_count': 'uint16',
                        'user_to_name': str,
                        'text': str,
                        'weights': 'uint16'
                    }
                },
                'w_kwargs': {}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        # if self.config.skip_output_check or not self.output:
        if not self.output:
            self.output['network'] = self.__create_network(self.input['stream'])

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __create_network(twstream):
        logger.info('create network')
        return None
