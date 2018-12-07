import logging

import helper
from ..pipeline_manager_base import PipelineManagerBase
from .profile_info import ProfileInfo
from .profile_metrics import ProfileMetrics
from .userevent_metrics import UserEventMetrics
from .user_stream import UserStream
from .ranking import Ranking

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

        us_input_stage = helper.pass_results_pipeline(
            (self.input, self.input_format), (pm_output, pm_output_format), ['event'])
        us = UserStream(self.config, us_input_stage[0], us_input_stage[1])
        us_output, us_output_format = us.execute()

        uem_input_stage = helper.pass_results_pipeline(
            (self.input, self.input_format), (us_output, us_output_format), ['event'])
        uem = UserEventMetrics(self.config, uem_input_stage[0], uem_input_stage[1])
        uem_output, uem_output_format = uem.execute()

        # r = Ranking(self.config, uem_input_stage[0], uem_input_stage[1])
        # r_output, r_output_format = r.execute()

        logger.info(f'END pipeline for {self.config.dataset_name}')

        # return r_output, r_output_format
        return uem_output, uem_output_format
