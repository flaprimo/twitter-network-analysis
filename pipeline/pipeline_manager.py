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

        pp = PreProcessing(self.config)
        pp_output = pp.execute()

        cd = CommunityDetection(self.config, pp_output)
        cd_output = cd.execute()

        m = Metrics(self.config, cd_output)
        m_output = m.execute()

        logger.info(f'END pipeline for {self.config.data_filename}')
