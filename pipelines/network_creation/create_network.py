import logging
import re
import helper
import pandas as pd
from datasources import PipelineIO

logger = logging.getLogger(__name__)


class CreateNetwork:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['stream'], stage_input, stage_input_format)
        self.output_prefix = 'cn'
        self.output_format = {
            'network': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'network'),
                'r_kwargs': {
                    'dtype': {
                        'from_username': str,
                        'to_username': str,
                        'text': str,
                    }
                },
                'w_kwargs': {}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.skip_output_check or not self.output:
            self.output['network'] = self.__create_network(self.input['stream'])

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __create_network(stream):
        logger.info('create network')

        tw_list = []
        for tw in stream:
            if tw['full_text'].startswith('RT'):
                from_username = tw['user']['screen_name'].lower()
                mentions = re.findall(r'@\w+', tw['full_text'])

                for user in mentions:
                    tw_record = {
                        'from_username': from_username,
                        'to_username': user.replace('@', '').lower(),
                        'text': tw['full_text'].replace('\n', ''),
                    }
                    tw_list.append(tw_record)

        tw_df = pd.DataFrame.from_records(tw_list, columns=['from_username', 'to_username', 'text'])

        logger.info('merge duplicates columns')
        logger.debug(helper.df_tostring(tw_df, 5))

        return tw_df
