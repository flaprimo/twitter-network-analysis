from datetime import datetime
import logging
import pandas as pd
from datasources.tw import tw
from pipelines.helper import str_to_list
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class ContextDetection(PipelineBase):
    def __init__(self, datasources, context_name):
        files = [
            {
                'stage_name': 'create_context',
                'file_name': 'context',
                'file_extension': 'csv',
                'file_prefix': context_name,
                'r_kwargs': {
                    'dtype': {
                        'name': str,
                        'start_date': str,
                        'end_date': str,
                        'location': str
                    },
                    'converters': {
                        'hashtags': str_to_list
                    },
                    'parse_dates': ['start_date', 'end_date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d'),
                    'index_col': 'name'
                }
            },
            {
                'stage_name': 'harvest_context',
                'file_name': 'stream',
                'file_extension': 'json',
                'file_prefix': context_name
            },
            {
                'stage_name': 'harvest_context',
                'file_name': 'stream_expanded',
                'file_extension': 'csv',
                'file_prefix': context_name,
                'r_kwargs': {
                    'dtype': {
                        'tw_id': int,
                        'user_name': str,
                        'date': str,
                        'text': str,
                        'lang': str,
                        'reply': str,
                        'no_likes': 'uint32',
                        'no_retweets': 'uint32',
                        'no_replies': 'uint32',
                        'is_retweet': bool,
                        'is_media': bool

                    },
                    'converters': {
                        'hashtags': str_to_list,
                        'urls': str_to_list,
                        'mentions': str_to_list,
                        'retweeted_hashtags': str_to_list
                    },
                    'parse_dates': ['date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__create_context, self.__harvest_context, self.__expand_context]
        self.context_name = context_name
        super(ContextDetection, self).__init__('context_detection', files, tasks, datasources)

    def __create_context(self):
        if not self.datasources.files.exists(
                'context_detection', 'create_context', 'context', 'csv', self.context_name):
            context = self.datasources.contexts.get_context(self.context_name)
            self.datasources.files.write(
                context, 'context_detection', 'create_context', 'context', 'csv', self.context_name)

    def __harvest_context(self):
        if not self.datasources.files.exists(
                'context_detection', 'harvest_context', 'stream', 'json', self.context_name):
            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.to_dict('records')[0]

            stream = tw.tw_api.premium_search(query=' OR '.join(context_record['hashtags']),
                                              since=context_record['start_date'],
                                              until=context_record['end_date'],
                                              n=200)
            self.datasources.files.write(
                stream, 'context_detection', 'harvest_context', 'stream', 'json', self.context_name)

    def __expand_context(self):
        if not self.datasources.files.exists(
                'context_detection', 'harvest_context', 'stream_expanded', 'csv', self.context_name):
            stream = self.datasources.files.read(
                'context_detection', 'harvest_context', 'stream', 'json', self.context_name)
            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.to_dict('records')[0]
            tw_df = pd.DataFrame.from_records([tw.tw_api.parse_tweet(raw_tw) for raw_tw in stream])
            users = list(set(tw_df['user_name'].tolist() + tw_df['mentions'].sum()))

            tw_df_expanded = pd.DataFrame.from_records(tw.tw_api.get_user_timelines(
                users, n=3200, from_date=context_record['start_date'], to_date=context_record['end_date']))

            if not tw_df_expanded.empty:
                tw_df_expanded = tw_df_expanded[
                    tw_df_expanded['hashtags']
                        .apply(lambda t: any(h in context_record['hashtags'] for h in t)) |
                    tw_df_expanded['retweeted_hashtags']
                        .apply(lambda t: any(h in context_record['hashtags'] for h in t))]

            tw_df_expanded = pd.concat([tw_df_expanded, tw_df], ignore_index=True) \
                .drop_duplicates(subset=['tw_id'], keep='first')

            self.datasources.files.write(
                tw_df_expanded, 'context_detection', 'harvest_context', 'stream_expanded', 'csv', self.context_name)
