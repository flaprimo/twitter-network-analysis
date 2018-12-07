import pandas as pd
import logging
import helper
from datasources import PipelineIO
from datasources.tw.helper import query_builder
from datasources.tw.tw import tw

logger = logging.getLogger(__name__)


class UserStream:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['nodes', 'event'], stage_input, stage_input_format)
        self.output_prefix = 'us'
        self.output_format = {
            'stream': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'stream'),
                'r_kwargs': {
                    'dtype': {
                        'tw_id': str,
                        'user_id': 'uint32',
                        'author': str,
                        'date': str,
                        'language': str,
                        'text': str,
                        'no_replies': 'uint32',
                        'no_retweets': 'uint32',
                        'no_likes': 'uint32'
                    },
                    'converters': {
                        'reply': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'hashtags': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'emojis': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'urls': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'mentions': lambda x: x.strip('[]').replace('\'', '').split(', ')
                    }
                },
                'w_kwargs': {'index': False}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.skip_output_check or not self.output:
            self.output['stream'] = self.__get_stream(self.input['nodes'], self.input['event'])

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __get_stream(nodes, event):
        logger.info('getting tw stream for users')
        event_record = event.reset_index().to_dict('records')[0]

        user_names = nodes['user_name'].drop_duplicates().tolist()

        streams = []
        for u in user_names:
            query = query_builder(
                people={'from': u},
                date={
                    'since': event_record['start_date'],
                    'until': event_record['end_date']
                })
            streams.extend(tw.tw_dynamic_scraper.search(query))

        df_stream = pd.DataFrame.from_records(streams)

        logger.debug(helper.df_tostring(df_stream, 5))

        return df_stream
