import logging
from ..pipeline_manager_base import PipelineManagerBase
from .profile_info import ProfileInfo
from .metrics import Metrics

logger = logging.getLogger(__name__)


class PipelineManager(PipelineManagerBase):
    def __init__(self, config, input_stage):
        super(PipelineManager, self).__init__(config, input_stage)
        logger.info(f'INIT pipeline for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.dataset_name}')

        pi = ProfileInfo(self.config, self.input, self.input_format)
        pi_output, pi_output_format = pi.execute()

        m = Metrics(self.config, pi_output, pi_output_format)
        m_output, m_output_format = m.execute()

        logger.info(f'END pipeline for {self.config.dataset_name}')

        return m_output, m_output_format
