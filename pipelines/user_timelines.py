import logging
import pandas as pd
from datetime import datetime, date
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
            },
            {
                'stage_name': 'filter_user_timelines',
                'file_name': 'filtered_user_timelines',
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
        tasks = [self.__get_user_timelines, self.__filter_user_timelines]
        super(UserTimelines, self).__init__('user_timelines', files, tasks, datasources)

    def __get_user_timelines(self):
        if not self.datasources.files.exists('user_timelines', 'get_user_timelines', 'user_timelines', 'csv'):
            cd_config = self.datasources.context_detection.get_config()

            rank_2 = self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')['user_name'] \
                .head(cd_config['top_no_users']).tolist()

            tw_df = pd.DataFrame.from_records(
                self.datasources.tw_api.get_user_timelines(
                    rank_2,
                    n=cd_config['max_no_tweets'],
                    from_date=datetime.strptime(cd_config['start_date'], '%Y-%m-%d').date(),
                    to_date=datetime.strptime(cd_config['end_date'], '%Y-%m-%d').date()
                ))

            self.datasources.files.write(tw_df, 'user_timelines', 'get_user_timelines', 'user_timelines', 'csv')

    def __filter_user_timelines(self):
        if not self.datasources.files.exists(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'get_user_timelines', 'user_timelines', 'csv')

            cd_config = self.datasources.context_detection.get_config()

            # limit users and tweets
            rank_2 = self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')['user_name'] \
                .head(cd_config['top_no_users'])
            user_timelines = user_timelines[user_timelines['user_name'].isin(rank_2)]
            n_tws = int(user_timelines.groupby('user_name').size().mean())
            user_timelines = user_timelines.groupby('user_name') \
                .apply(lambda x: x.sort_values(by='date', ascending=True).head(n_tws)).reset_index(drop=True)

            self.datasources.files.write(
                user_timelines, 'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')
