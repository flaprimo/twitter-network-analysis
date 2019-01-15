from datetime import datetime
import pandas as pd

import helper
from pipelines import event_detection, network_creation, community_detection, profiling
import logging
import time

logging.basicConfig(level=logging.DEBUG, filename='logs/debug.log', format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, project_name, cd_config):
        self.project_name = project_name
        self.project_path = f'input/{project_name}.csv'
        self.events = self.__parse_events(self.project_path)
        self.cd_config = cd_config

        logger.info(f'INIT orchestrator for {self.project_name}')

    def execute(self):
        start_time = time.time()
        logger.info(f'EXEC orchestrator for {self.project_name}')

        results = self.sequential_exec()

        logger.info(f'END orchestrator for {self.project_name}')
        logger.debug(f'elapsed time: {round(time.time() - start_time, 4)} s')

        return results

    def sequential_exec(self):
        results = {}
        for e in self.events.reset_index().iterrows():
            e_name = e[1]['name']
            e_df = pd.DataFrame([e[1]]).set_index('name')

            # EVENT DETECTION
            ed_config = event_detection.Config(self.project_name, e_name)
            ed_results = self.ed_pipeline(ed_config, ({'event': e_df}, {'event': {}}))

            # NETWORK CREATION
            nc_config = network_creation.Config(self.project_name, e_name)
            nc_results = self.nc_pipeline(nc_config, ed_results)

            # COMMUNITY DETECTION
            cd_config = community_detection.Config(self.project_name, e_name, self.cd_config)
            cd_results = self.cd_pipeline(cd_config, nc_results)

            # PROFILING
            p_input_stage = helper.pass_results_pipeline(ed_results, cd_results, ['event'])
            p_config = profiling.Config(self.project_name, e_name, cd_config.postfix)
            p_results = self.p_pipeline(p_config, p_input_stage)

            # only store output format for improved memory management
            results[e_name] = {
                'event_detection': ed_results[1],
                'network_creation': nc_results[1],
                'community_detection': cd_results[1],
                'profiling': p_results[1]
            }

        return results

    @staticmethod
    def ed_pipeline(config, input_stage):
        p = event_detection.PipelineManager(config, input_stage)
        return p.execute()

    @staticmethod
    def nc_pipeline(config, input_stage):
        p = network_creation.PipelineManager(config, input_stage)
        return p.execute()

    @staticmethod
    def cd_pipeline(config, input_stage):
        cd = community_detection.PipelineManager(config, input_stage)
        return cd.execute()

    @staticmethod
    def p_pipeline(config, input_stage):
        p = profiling.PipelineManager(config, input_stage)
        return p.execute()

    @staticmethod
    def __parse_events(events_path):
        events = pd.read_csv(events_path,
                             dtype={
                                 'name': str,
                                 'start_date': str,
                                 'end_date': str,
                                 'location': str
                             },
                             converters={'hashtags': lambda x: x.split(' ')},
                             parse_dates=['start_date', 'end_date'], index_col='name',
                             date_parser=lambda x: datetime.strptime(x, '%Y-%m-%d'))
        events['start_date'] = events['start_date'].apply(lambda x: x.date())
        events['end_date'] = events['end_date'].apply(lambda x: x.date())

        return events


def main():
    project_name = 'uk_healthcare'

    cd_config = ('infomap', {})
    # cd_config = ('demon', {
    #     'epsilon': 0.25,
    #     'min_community_size': 3
    # })

    o = Orchestrator(project_name, cd_config)
    o.execute()


if __name__ == "__main__":
    main()
