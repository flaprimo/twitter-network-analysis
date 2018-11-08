import logging
from datasources.pipeline_io import PipelineIO

logger = logging.getLogger(__name__)


class Persist:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['data'], stage_input, stage_input_format)
        self.output_prefix = ''
        self.output_format = {}
        self.output = None
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['edges'] = self.__persist_user(self.input['nodes'])

            PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.data_filename}')

        return self.output, self.output_format

    @staticmethod
    def __persist_user(nodes):
        return None
