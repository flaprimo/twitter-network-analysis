import logging
import pandas as pd
from datasources.tw.helper import query_builder
from datasources.tw.tw import tw
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class UserEventMetrics(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name':  'get_user_stream',
                'file_name':  'stream',
                'file_extension':  'csv',
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
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_user_stream',
                'file_name': 'metrics',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        # 'user_id': 'uint32',
                        'user_name': str,
                        'topical_attachment': 'float32',
                        'topical_focus': 'float32',
                        'topical_strength': 'float32'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__get_user_stream]
        super(UserEventMetrics, self).__init__('userevent_metrics', files, tasks, datasources, file_prefix)

    def __get_user_stream(self):
        if not self.datasources.files.exists(
                'userevent_metrics', 'get_user_stream', 'stream', 'csv', self.context_name):
            profile_info = self.datasources.files.read(
                'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)

            user_names = profile_info['user_name'].tolist()
            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.reset_index().to_dict('records')[0]

            streams = []
            for u in user_names:
                query = query_builder(
                    people={'from': u},
                    date={
                        'since': context_record['start_date'],
                        'until': context_record['end_date']
                    })
                stream = tw.tw_dynamic_scraper.search(query)

                # only keeps tws from current user_name
                stream = [s for s in stream if s['author'] == u]

                streams.extend(stream)
            # can try merging for having users_id and filtering proper users
            df_stream = pd.DataFrame.from_records(streams)

            self.datasources.files.write(
                df_stream, 'userevent_metrics', 'get_user_stream', 'stream', 'csv', self.context_name)
