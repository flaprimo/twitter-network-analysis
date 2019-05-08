import logging
import pandas as pd
from datetime import datetime
from .pipeline_base import PipelineBase
from .helper import str_to_list

logger = logging.getLogger(__name__)


class UserTimelines(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_user_timelines',
                'file_name': 'user_timelines',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'tw_id': int,
                        'user_name': str,
                        'date': str,
                        'text': str,
                        'lang': str,
                        'no_likes': 'uint32',
                        'no_retweets': 'uint32',
                        'no_replies': 'uint32',
                        'is_retweet': bool,
                        'is_media': bool,

                    },
                    'converters': {
                        'hashtags': str_to_list,
                        'urls': str_to_list,
                        'mentions': str_to_list,
                        'replies': str_to_list
                    },
                    'parse_dates': ['date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__get_user_timelines]
        super(UserTimelines, self).__init__('user_timelines', files, tasks, datasources)

    def __get_user_timelines(self):
        if not self.datasources.files.exists('user_timelines', 'get_user_timelines', 'user_timelines', 'csv'):
            rank_2 = self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')['user_name']\
                .head(3000).tolist()

            tw_df = pd.DataFrame.from_records(
                self.datasources.tw_api.get_user_timelines(
                    rank_2, n=3200, from_date=self.datasources.contexts.contexts['start_date'].min(),
                    to_date=self.datasources.contexts.contexts['end_date'].max()))

            self.datasources.files.write(tw_df, 'user_timelines', 'get_user_timelines', 'user_timelines', 'csv')
