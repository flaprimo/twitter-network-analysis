import logging
from ..pipeline_manager_base import PipelineManagerBase
from .persist import Persist

logger = logging.getLogger(__name__)


class PPipelineManagerBase(PipelineManagerBase):
    def __init__(self, config, input_stage):
        super(PPipelineManagerBase, self).__init__(config, input_stage)
        logger.info(f'INIT pipeline for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.dataset_name}')

        p = Persist(self.config, self.input, self.input_format)
        p_output, p_output_format = p.execute()

        logger.info(f'END pipeline for {self.config.dataset_name}')
