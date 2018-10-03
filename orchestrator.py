from concurrent.futures import ProcessPoolExecutor
from pipeline.pipeline_manager import PipelineManager
from config import Config
import logging
import time

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, datasets):
        self.datasets = datasets
        self.configs = [Config(data_filename=d) for d in datasets]
        logger.info(f'INIT orchestrator for {self.datasets}')

    def execute(self):
        start_time = time.time()
        logger.info(f'EXEC orchestrator for {self.datasets}')

        with ProcessPoolExecutor() as executor:
            results = list(executor.map(self.pipeline, self.configs))

        for r in results:
            print(r)

        logger.info(f'END orchestrator for {self.datasets}')
        logger.debug(f'elapsed time: {round(time.time() - start_time, 4)} s')

    @staticmethod
    def pipeline(config):
        p = PipelineManager(config)
        p.execute()

        return f'finished {config.data_filename}!'


def main():
    datasets = ['#GTC18', '#IPAW2018', '#NIPS2017', '#provenanceweek', '#TCF2018', 'ECMLPKDD2018',
                'emnlp2018', 'kdd', 'msignite2018', 'ona18', 'recsys']
    o = Orchestrator(datasets)
    o.execute()


if __name__ == "__main__":
    main()
