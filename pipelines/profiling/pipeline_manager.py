import logging
from ..pipeline_manager_base import PipelineManagerBase
from .profile_info import ProfileInfo
from .profile_metrics import ProfileMetrics
from .userevent_metrics import UserEventMetrics

logger = logging.getLogger(__name__)


class PipelineManager(PipelineManagerBase):
    def __init__(self, config, input_stage):
        super(PipelineManager, self).__init__(config, input_stage)
        logger.info(f'INIT pipeline for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.dataset_name}')

        pi = ProfileInfo(self.config, self.input, self.input_format)
        pi_output, pi_output_format = pi.execute()

        pm = ProfileMetrics(self.config, pi_output, pi_output_format)
        pm_output, pm_output_format = pm.execute()

        uem = UserEventMetrics(self.config, pm_output, pm_output_format)
        uem_output, uem_output_format = uem.execute()

        logger.info(f'END pipeline for {self.config.dataset_name}')

        return uem_output, uem_output_format
