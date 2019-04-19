import logging
from datetime import datetime
import pandas as pd
from datasources import tw
from pipelines.helper import str_to_list
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class UserContextMetrics(PipelineBase):
    def __init__(self, datasources, context_name):
        files = [
            {
                'stage_name': 'get_user_stream',
                'file_name': 'stream',
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
            },
            {
                'stage_name': 'compute_metrics',
                'file_name': 'usercontext_metrics',
                'file_extension': 'csv',
                'file_prefix': context_name,
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
        tasks = [self.__get_user_stream, self.__compute_metrics]
        self.context_name = context_name
        super(UserContextMetrics, self).__init__('usercontext_metrics', files, tasks, datasources)

    def __get_user_stream(self):
        if not self.datasources.files.exists(
                'usercontext_metrics', 'get_user_stream', 'stream', 'csv', self.context_name):
            profile_info = self.datasources.files.read(
                'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)

            user_names = profile_info['user_name'].tolist()
            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.reset_index().to_dict('records')[0]

            tw_df = pd.DataFrame.from_records(tw.tw_api.get_user_timelines(
                user_names, n=3200, from_date=context_record['start_date'], to_date=context_record['end_date']))

            self.datasources.files.write(
                tw_df, 'usercontext_metrics', 'get_user_stream', 'stream', 'csv', self.context_name)

    def __compute_metrics(self):
        if not self.datasources.files.exists(
                'usercontext_metrics', 'compute_metrics', 'usercontext_metrics', 'csv', self.context_name):
            stream = self.datasources.files.read(
                'usercontext_metrics', 'get_user_stream', 'stream', 'csv', self.context_name)
            nodes = self.datasources.files.read(
                'profile_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name)

            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.reset_index().to_dict('records')[0]

            stream['tw_ontopic'] =\
                stream['hashtags'].apply(lambda t: any(h in context_record['hashtags'] for h in t)) |\
                stream['retweeted_hashtags'].apply(lambda t: any(h in context_record['hashtags'] for h in t))

            stream['link_ontopic'] = stream[stream['tw_ontopic']]['urls'].apply(lambda t: t != [''])
            stream['link_ontopic'].fillna(False, inplace=True)

            # topical attachment
            def topical_attachment_alg(tw_ontopic, tw_offtopic, link_ontopic, link_offtopic):
                return (tw_ontopic + link_ontopic) / (tw_offtopic + link_offtopic + 1)

            topical_attachment = \
                stream[['user_name', 'tw_ontopic', 'link_ontopic']].groupby('user_name') \
                .apply(lambda x: topical_attachment_alg(x['tw_ontopic'].sum(), (~x['tw_ontopic']).sum(),
                                                        x['link_ontopic'].sum(), (~x['link_ontopic']).sum())) \
                .to_frame().rename(columns={0: 'topical_attachment'})

            # topical focus
            def topical_focus_alg(t_ontopic, t_offtopic):
                return t_ontopic / (t_offtopic + 1)

            topical_focus = \
                stream[['user_name', 'tw_ontopic']].groupby('user_name') \
                .apply(lambda x: topical_focus_alg(x['tw_ontopic'].sum(), (~x['tw_ontopic']).sum())) \
                .to_frame().rename(columns={0: 'topical_focus'})

            # topical strength
            def topical_strength_alg(link_ontopic, link_offtopic, rtw_ontopic, rtw_offtopic):
                import math
                return (link_ontopic * math.log10(link_ontopic + rtw_ontopic + 1)) / \
                       (link_offtopic * math.log10(link_offtopic + rtw_offtopic + 1) + 1)

            topical_strength = \
                stream[['user_name', 'tw_ontopic', 'link_ontopic', 'no_retweets']].groupby('user_name') \
                .apply(lambda x: topical_strength_alg(x['link_ontopic'].sum(), (~x['link_ontopic']).sum(),
                                                      x[x['tw_ontopic']]['no_retweets'].sum(),
                                                      x[~x['tw_ontopic']]['no_retweets'].sum())) \
                .to_frame().rename(columns={0: 'topical_strength'})

            usercontexts = topical_attachment \
                .merge(topical_focus, left_index=True, right_index=True) \
                .merge(topical_strength, left_index=True, right_index=True) \
                .reset_index()

            # add missing nodes
            usercontexts = usercontexts.merge(nodes[['user_name']], left_on='user_name', right_on='user_name',
                                              how='outer', sort='True').fillna(0)

            self.datasources.files.write(
                usercontexts, 'usercontext_metrics', 'compute_metrics', 'usercontext_metrics', 'csv', self.context_name)
