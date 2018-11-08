from concurrent.futures import ProcessPoolExecutor
from pipelines import community_detection, profiling
import logging
import time

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, datasets, cd_config):
        self.datasets = datasets
        self.cd_configs = [community_detection.Config(d, cd_config) for d in datasets]
        self.p_configs = [profiling.Config(c.data_filename, c.postfix) for c in self.cd_configs]
        logger.info(f'INIT orchestrator for {self.datasets}')

    def execute(self):
        start_time = time.time()
        logger.info(f'EXEC orchestrator for {self.datasets}')

        # COMMUNITY DETECTION
        with ProcessPoolExecutor() as executor:
            cd_results = {c.data_filename: r
                          for c, r in zip(self.cd_configs, executor.map(self.cd_pipeline, self.cd_configs))}

        # PROFILING
        # p_results = {c.data_filename: self.p_pipeline(c, cd_results[c.data_filename])
        #              for c in self.p_configs}

        # for r in p_results:
        #     print(r)

        logger.info(f'END orchestrator for {self.datasets}')
        logger.debug(f'elapsed time: {round(time.time() - start_time, 4)} s')

        return cd_results

    @staticmethod
    def cd_pipeline(config):
        p = community_detection.PipelineManager(config)
        return p.execute()

    @staticmethod
    def p_pipeline(config, input_stage):
        p = profiling.PipelineManager(config, input_stage)
        p.execute()

        return f'finished profiling on {config.data_filename}!'


def main():
    datasets = ['#GTC18', '#IPAW2018', '#NIPS2017', '#provenanceweek', '#TCF2018', 'ECMLPKDD2018',
                'emnlp2018', 'kdd', 'msignite2018', 'ona18', 'recsys']
    # datasets = ['kdd']
    # cd_config = ('infomap', {})
    cd_config = ('demon', {
        'epsilon': 0.25,
        'min_community_size': 3
    })

    o = Orchestrator(datasets, cd_config)
    o.execute()


if __name__ == "__main__":
    main()
