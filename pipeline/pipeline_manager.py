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
                'r_kwargs': {'dtype': self.config.data_type['csv_data']},
                'w_kwargs': {}
            }
        }
        pp = PreProcessing(self.config, stage_input_format=data_input_format)
        pp_output, pp_output_format = pp.execute()

        cd = CommunityDetection(self.config, pp_output, pp_output_format)
        cd_output, cd_output_format = cd.execute()

        m = Metrics(self.config, cd_output)
        m_output = m.execute()

        logger.info(f'END pipeline for {self.config.data_filename}')
