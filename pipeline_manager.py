from stages2 import PreProcessing, CommunityDetection
from config import Config
import logging

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class PipelineManager:
    def __init__(self, config):
        self.config = config
        self.execute()
        logger.info(f'INIT pipeline for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.data_filename}')

        pp = PreProcessing(self.config)
        pp_output = pp.execute()

        cd = CommunityDetection(self.config, pp_output)
        cd_output = cd.execute()


def main():
    PipelineManager(Config(data_filename='ll'))


if __name__ == "__main__":
    main()
