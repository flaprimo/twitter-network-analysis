from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import pandas as pd

import helper
from pipelines import community_detection, profiling, event_detection
import logging
import time

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, project_name, events, cd_config):
        self.project_name = project_name
        self.events = events

        self.ed_configs = [event_detection.Config(self.project_name, e) for e in events.index]
        self.cd_configs = [community_detection.Config(self.project_name, e, cd_config) for e in events.index]
        self.p_configs = [profiling.Config(self.project_name, c.data_filename, c.postfix) for c in self.cd_configs]
        logger.info(f'INIT orchestrator for {self.project_name}')

    def execute(self):
        start_time = time.time()
        logger.info(f'EXEC orchestrator for {self.project_name}')

        # EVENT_DETECTION
        ed_results = {}
        for c in self.ed_configs:
            print(c.data_filename)
            print(self.events[self.events.index == c.data_filename])

            stage_input = {'event': self.events[self.events.index == c.data_filename]}
            stage_input_format = {
                    'event': {
                        'name': str,
                        'start_date': str,
                        'end_date': str,
                        'location': str,
                        'hashtags': str
                    }
                }

            ed_results[c.data_filename] = self.ed_pipeline(c, (stage_input, stage_input_format))

        for r in ed_results:
            print(r)

        # COMMUNITY DETECTION
        # with ProcessPoolExecutor() as executor:
        #     cd_results = {c.data_filename: r
        #                   for c, r in zip(self.cd_configs, executor.map(self.cd_pipeline, self.cd_configs))}

        # PROFILING
        # p_results = {c.data_filename: self.p_pipeline(c, cd_results[c.data_filename])
        #              for c in self.p_configs}

        # for r in p_results:
        #     print(r)

        logger.info(f'END orchestrator for {self.project_name}')
        logger.debug(f'elapsed time: {round(time.time() - start_time, 4)} s')

        # return cd_results

    @staticmethod
    def ed_pipeline(config, input_stage):
        p = event_detection.PipelineManager(config, input_stage)
        return p.execute()

    @staticmethod
    def cd_pipeline(config):
        cd = community_detection.PipelineManager(config)
        return cd.execute()

    @staticmethod
    def p_pipeline(config, input_stage):
        p = profiling.PPipelineManagerBase(config, input_stage)
        p.execute()

        return f'finished profiling on {config.data_filename}!'


def main():
    events_dtype = {
        'name': str,
        'start_date': str,
        'end_date': str,
        'location': str,
        'hashtags': str
    }
    events = pd.read_csv('input/uk_healthcare.csv', dtype=events_dtype,
                         parse_dates=['start_date', 'end_date'], index_col='name',
                         date_parser=lambda x: datetime.strptime(x, '%Y-%m-%d'))
    events['start_date'] = events['start_date'].apply(lambda x: x.date())
    events['end_date'] = events['end_date'].apply(lambda x: x.date())
    # print(helper.df_tostring(events))

    # datasets = ['#GTC18', '#IPAW2018', '#NIPS2017', '#provenanceweek', '#TCF2018', 'ECMLPKDD2018',
    #             'emnlp2018', 'kdd', 'msignite2018', 'ona18', 'recsys']
    # datasets = ['kdd']
    # cd_config = ('infomap', {})

    cd_config = ('demon', {
        'epsilon': 0.25,
        'min_community_size': 3
    })

    o = Orchestrator('uk_healthcare', events, cd_config)
    o.execute()


if __name__ == "__main__":
    main()
