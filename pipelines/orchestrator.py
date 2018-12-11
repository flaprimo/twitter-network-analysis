from concurrent.futures import ProcessPoolExecutor
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

        self.ed_configs = [event_detection.Config(self.project_name, e) for e in self.events.index]
        self.nc_configs = [network_creation.Config(self.project_name, e) for e in self.events.index]
        self.cd_configs = [community_detection.Config(self.project_name, e, cd_config) for e in self.events.index]
        self.p_configs = [profiling.Config(self.project_name, c.dataset_name, c.postfix) for c in self.cd_configs]
        logger.info(f'INIT orchestrator for {self.project_name}')

    def execute(self):
        start_time = time.time()
        logger.info(f'EXEC orchestrator for {self.project_name}')

        # EVENT_DETECTION
        ed_results = {}
        for c in self.ed_configs:
            nc_input = {'event': self.events[self.events.index == c.dataset_name]}
            nc_input_format = {'event': {}}

            ed_results[c.dataset_name] = self.ed_pipeline(c, (nc_input, nc_input_format))

        # NETWORK CREATION
        nc_results = {c.dataset_name: self.nc_pipeline(c, ed_results[c.dataset_name])
                      for c in self.nc_configs}
        # nc_results = helper.remove_results_orchestrator(nc_results)

        # COMMUNITY DETECTION
        # with ProcessPoolExecutor() as executor:
        #     cd_results = {c.dataset_name: r
        #                   for c, r in zip(self.cd_configs, executor.map(self.cd_pipeline, self.cd_configs, nc_results))}

        cd_results = {c.dataset_name: self.cd_pipeline(c, nc_results[c.dataset_name])
                      for c in self.cd_configs}
        # cd_results = helper.remove_results_orchestrator(cd_results)

        # PROFILING
        p_input_stage = helper.pass_results_orchestrator(ed_results, cd_results, ['event'])
        p_results = {c.dataset_name: self.p_pipeline(c, p_input_stage[c.dataset_name])
                     for c in self.p_configs}

        logger.info(f'END orchestrator for {self.project_name}')
        logger.debug(f'elapsed time: {round(time.time() - start_time, 4)} s')

        return cd_results

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
    # project_name = 'uk_healthcare'
    # project_name = 'datascience_conferences'
    project_name = 'datascience_conferences_single'

    # cd_config = ('infomap', {})
    cd_config = ('demon', {
        'epsilon': 0.25,
        'min_community_size': 3
    })

    o = Orchestrator(project_name, cd_config)
    o.execute()


if __name__ == "__main__":
    main()
