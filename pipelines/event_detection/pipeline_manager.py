import logging
from ..pipeline_manager_base import PipelineManagerBase
from .create_event import CreateEvent

logger = logging.getLogger(__name__)


class PipelineManager(PipelineManagerBase):
    def __init__(self, config, input_stage):
        super(PipelineManager, self).__init__(config, input_stage)
        logger.info(f'INIT pipeline for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.dataset_name}')

        ce = CreateEvent(self.config, self.input, self.input_format)
        ce_output, ce_output_format = ce.execute()

        logger.info(f'END pipeline for {self.config.dataset_name}')

        return ce_output, ce_output_format