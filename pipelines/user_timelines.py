import logging
import re
import pandas as pd
from datasources import tw
from datetime import datetime
import pytz
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class UserTimelines(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_user_timelines',
                'file_name': 'stream',
                'file_extension': 'json'
            },
            {
                'stage_name': 'parse_user_timelines',
                'file_name': 'user_timelines',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_name': str,
                        'date': str,
                        'text': str,
                        'likes': 'uint32',
                        'retweets': 'uint32',
                        'is_retweet': bool
                    },
                    'converters': {
                        'hashtags': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'urls': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'mentions': lambda x: x.strip('[]').replace('\'', '').split(', '),
                    },
                    'parse_dates': 'date',
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_hashtags',
                'file_name': 'hashtags',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'hashtags': str
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__get_user_timelines, self.__parse_user_timelines, self.__get_hashtags]
        super(UserTimelines, self).__init__('user_timelines', files, tasks, datasources)

    def __get_user_timelines(self):
        if not self.datasources.files.exists('user_timelines', 'get_user_timelines', 'stream', 'json'):
            rank_2 = self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')['user_name'].head(1000).tolist()

            stream = tw.tw_api.get_user_timelines(rank_2, 50)

            self.datasources.files.write(stream, 'user_timelines', 'get_user_timelines', 'stream', 'json')

    def __parse_user_timelines(self):
        if not self.datasources.files.exists('user_timelines', 'parse_user_timelines', 'user_timelines', 'csv'):
            stream = self.datasources.files.read('user_timelines', 'get_user_timelines', 'stream', 'json')

            tw_list = []
            for s in stream:
                for t in s['stream']:
                    tw_record = {
                        'user_name': s['user_name'],
                        'date': datetime.strptime(t['created_at'], '%a %b %d %H:%M:%S %z %Y')
                        .astimezone(pytz.UTC).replace(tzinfo=None),
                        'text': t['text'],
                        'likes': t['favorite_count'],
                        'retweets': t['retweet_count'],
                        'is_retweet': 'retweeted_status' in t,
                        'hashtags': ['#' + h['text'].lower() for h in t['entities']['hashtags']],
                        'mentions': [m['screen_name'].lower() for m in t['entities']['user_mentions']],
                        'urls': [u['expanded_url'] for u in t['entities']['urls']]
                    }

                    # text cleanup
                    tw_record['text'] = re.sub(r'^RT @\w+: ', '', tw_record['text'])
                    tw_record['text'] = re.sub(r'https*:\/\/t.co\/\w+', '', tw_record['text'])
                    tw_record['text'] = re.sub(r'(@|#)\w*', '', tw_record['text'])
                    tw_record['text'] = re.sub(r'\n|\t|  +', ' ', tw_record['text'])
                    tw_record['text'] = re.sub(r'(\w+…|…)$', '', tw_record['text'])
                    tw_record['text'] = re.sub(r'  +', '', tw_record['text'])
                    tw_record['text'] = tw_record['text'].strip()

                    tw_list.append(tw_record)

            tw_df = pd.DataFrame.from_records(tw_list)

            self.datasources.files.write(tw_df, 'user_timelines', 'parse_user_timelines', 'user_timelines', 'csv')

    def __get_hashtags(self):
        if not self.datasources.files.exists('user_timelines', 'get_hashtags', 'hashtags', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'parse_user_timelines', 'user_timelines', 'csv')

            hashtags = list(set([h for h_sublist in user_timelines['hashtags'].tolist() for h in h_sublist]))

            tw_df = pd.DataFrame({'hashtag': hashtags})

            self.datasources.files.write(tw_df, 'user_timelines', 'get_hashtags', 'hashtags', 'csv')
