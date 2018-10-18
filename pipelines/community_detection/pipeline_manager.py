from .pre_processing import PreProcessing
from .community_detection import CommunityDetection
from .metrics import Metrics

import logging

logger = logging.getLogger(__name__)


class PipelineManager:
    def __init__(self, config):
        self.config = config
        logger.info(f'INIT pipeline for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.data_filename}')
        data_input_format = {
            'data': {
                'type': 'pandas',
                'path': self.config.data_path,
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
        pp = PreProcessing(self.config, stage_input_format=data_input_format)
        pp_output, pp_output_format = pp.execute()

        cd = CommunityDetection(self.config, pp_output, pp_output_format)
        cd_output, cd_output_format = cd.execute()

        m = Metrics(self.config, cd_output, cd_output_format)
        m_output, m_output_format = m.execute()

        logger.info(f'END pipeline for {self.config.data_filename}')

        return m_output, m_output_format
