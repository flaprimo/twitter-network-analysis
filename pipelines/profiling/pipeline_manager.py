import logging
from .persist import Persist

logger = logging.getLogger(__name__)


class PipelineManager:
    def __init__(self, config, input_stage):
        self.config = config
        self.input = input_stage[0]
        self.input_format = input_stage[1]
        logger.info(f'INIT pipeline for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.data_filename}')

        p = Persist(self.config, self.input, self.input_format)
        p_output, p_output_format = p.execute()

        logger.info(f'END pipeline for {self.config.data_filename}')
