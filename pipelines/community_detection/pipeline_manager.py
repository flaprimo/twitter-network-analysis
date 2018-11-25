from pipelines.pipeline_manager_base import PipelineManagerBase
from .community_detection import CommunityDetection
from .metrics import Metrics
import logging

logger = logging.getLogger(__name__)


class PipelineManager(PipelineManagerBase):
    def __init__(self, config, input_stage):
        super(PipelineManager, self).__init__(config, input_stage)
        logger.info(f'INIT pipeline for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.data_filename}')

        cd = CommunityDetection(self.config, self.input, self.input_format)
        cd_output, cd_output_format = cd.execute()

        m = Metrics(self.config, cd_output, cd_output_format)
        m_output, m_output_format = m.execute()

        logger.info(f'END pipeline for {self.config.data_filename}')

        return m_output, m_output_format
