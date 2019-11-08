import logging
import numpy as np
import networkx as nx
import pandas as pd
from datetime import datetime
from pipelines.helper import str_to_list

from scipy.signal import find_peaks

from pipelines.pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class ContextDetector(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'hashtags_frequency',
                'file_name': 'hashtags_frequency',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'hashtag': str,
                        'date': str,
                        'count': 'float32'
                    },
                    'parse_dates': ['date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d').date()
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'find_peaks',
                'file_name': 'hashtags_peaks',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'hashtag': str,
                        'peak_value': 'float32',
                        'peak_width': 'uint8',
                        'peak_date': str,
                        'start_date': str,
                        'end_date': str
                    },
                    'parse_dates': ['peak_date', 'start_date', 'end_date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d').date()
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_ranked_users_with_hashtags',
                'file_name': 'ranked_users_hashtags',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_name': str,
                        'rank': 'float32',
                        'weight': 'uint16'
                    },
                    'converters': {
                        'hashtags': str_to_list
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_new_contexts',
                'file_name': 'new_contexts',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'hashtags': str,
                        'start_date': str,
                        'end_date': str
                    },
                    'converters': {
                        'context_hashtags': str_to_list
                    },
                    'parse_dates': ['start_date', 'end_date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date()
                }
            }
        ]
        tasks = [self.__hashtags_frequency, self.__find_peaks,
                 self.__get_ranked_users_with_hashtags, self.get_new_contexts]
        super(ContextDetector, self).__init__(
            'context_detector', files, tasks, datasources)

    def __hashtags_frequency(self):
        if not self.datasources.files.exists('context_detector', 'hashtags_frequency', 'hashtags_frequency', 'csv'):
            from scipy.stats import zscore
            tweets = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')[['date', 'hashtags']] \
                .explode('hashtags').dropna().rename(columns={'hashtags': 'hashtag'})
            tweets = tweets.groupby('hashtag').resample(
                'D', on='date').size().to_frame('count').reset_index()

            # subtract mean and retain only counts greater than 0
            tweets = tweets.loc[tweets['count'] > 0]
            tweets['count'] = zscore(tweets['count'])
            tweets = tweets.loc[tweets['count'] > 0]

            self.datasources.files.write(
                tweets, 'context_detector', 'hashtags_frequency', 'hashtags_frequency', 'csv')

    def __find_peaks(self):
        if not self.datasources.files.exists('context_detector', 'find_peaks', 'hashtags_peaks', 'csv'):
            # get bounds of the peak
            def get_bound(peak, t_series, t, direction):
                k = 0
                bound = peak
                current = bound - 1 if direction == 'l' else bound + 1

                while ((current > 0 and direction == 'l') or
                       (current < t_series.size - 1 and direction == 'r')) \
                        and k <= t:
                    current = bound - k - 1 if direction == 'l' else bound + k + 1
                    if t_series[current] > 0:
                        bound = current
                        k = 0
                    else:
                        k += 1

                return bound

            hashtags = self.datasources.files.read(
                'context_detector', 'hashtags_frequency', 'hashtags_frequency', 'csv')

            hashtag_peaks = []
            for hashtag, timeline in hashtags.groupby('hashtag'):
                timeline = timeline.set_index('date')['count']
                timeline = timeline.reindex(pd.date_range(timeline.index.min(), timeline.index.max()), fill_value=0)
                peaks = find_peaks(
                    timeline, height=timeline[timeline > 0].quantile(.9))[0]

                if peaks.size:
                    tolerance = 2

                    for p in peaks:
                        left_bound = get_bound(p, timeline, tolerance, 'l')
                        right_bound = get_bound(p, timeline, tolerance, 'r')

                        hashtag_peaks.append({
                            'hashtag': hashtag,
                            'peak_value': timeline.iloc[p],
                            'peak_date': timeline.index[p].date(),
                            'peak_width': right_bound - left_bound + 1,
                            'start_date': timeline.index[left_bound].date(),
                            'end_date': timeline.index[right_bound].date()
                        })

            hashtag_peaks = pd.DataFrame(hashtag_peaks) \
                .sort_values(['hashtag', 'peak_value', 'peak_width'], ascending=[True, False, False]) \
                .drop_duplicates(['hashtag', 'start_date', 'end_date'], keep='first').reset_index(drop=True)

            self.datasources.files.write(
                hashtag_peaks, 'context_detector', 'find_peaks', 'hashtags_peaks', 'csv')

            # find peaks
            # https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.peak_widths.html#scipy.signal.peak_widths

    def __get_ranked_users_with_hashtags(self):
        if not self.datasources.files.exists(
                'context_detector', 'get_ranked_users_with_hashtags', 'ranked_users_hashtags', 'csv'):
            rank_2 = self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')[['user_name', 'rank']]\
                .set_index('user_name', drop=True)
            user_hashtag_network = self.datasources.files.read(
                'bipartite_graph', 'get_user_hashtag_network', 'user_hashtag_network', 'csv')

            user_hashtag_network = user_hashtag_network.groupby('user_name')['hashtag'].apply(list) \
                .rename('hashtags').to_frame()

            ranked_users_hashtags = user_hashtag_network.merge(rank_2, left_index=True, right_index=True) \
                .sort_values(by='rank', ascending=False).reset_index()

            ranked_users_hashtags['weight'] = list(
                reversed(range(1, len(ranked_users_hashtags)+1)))

            self.datasources.files.write(
                ranked_users_hashtags,
                'context_detector', 'get_ranked_users_with_hashtags', 'ranked_users_hashtags', 'csv')

    def get_new_contexts(self):
        if not self.datasources.files.exists('context_detector', 'get_new_contexts', 'new_contexts', 'csv'):
            graph = self.datasources.files.read(
                'bipartite_community_detection', 'find_communities', 'multiplex_graph', 'gexf')
            hashtag_peaks = self.datasources.files.read(
                'context_detector', 'find_peaks', 'hashtags_peaks', 'csv')
            ranked_users_hashtags = self.datasources.files.read(
                'context_detector', 'get_ranked_users_with_hashtags', 'ranked_users_hashtags', 'csv')

            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)

            nx.set_node_attributes(graph, dict(nx.degree(graph)), 'degree')

            hashtag_communities = pd.DataFrame(
                [{'hashtag': attr['name'],
                  'community': attr['community'],
                  'degree': attr['degree']}
                 for i, attr in graph.nodes(data=True)
                 if attr['bipartite'] == 1]).set_index('hashtag', drop=True)

            hashtag_peaks = hashtag_peaks.merge(
                hashtag_communities, left_on='hashtag', right_index=True)

            # get topic of community
            community_topic = hashtag_peaks.groupby('community') \
                .apply(lambda x: x['hashtag'][x['degree'] == x['degree'].max()].values[0]) \
                .rename('community_topic')
            hashtag_peaks = hashtag_peaks.merge(
                community_topic, left_on='community', right_index=True)

            # get correlations
            hashtag_peaks.drop_duplicates(
                subset='hashtag', keep=False, inplace=True)
            hashtag_peaks['date_range'] = \
                hashtag_peaks.apply(lambda x: pd.date_range(
                    x['start_date'].date(), x['end_date'].date()), axis=1)

            corr_groups = {}
            for name, group in hashtag_peaks.groupby('community_topic'):
                g = group.set_index('hashtag', drop=True)['date_range']
                corr = g.apply(lambda x: g.iloc[x.name:]
                               .apply(lambda y: np.intersect1d(x, y).size / min(x.size, y.size)))\
                    .apply(lambda x: x > .5)
                corr = corr.drop_duplicates().T
                corr = corr.apply(
                    lambda x: [corr[x.name][corr[x.name]].index.tolist()]).reset_index(drop=True)
                corr = corr.values.tolist()
                # hack to flatten lists
                corr = [item for sublist in corr for item in sublist]
                corr_groups[name] = corr
            corr_groups = pd.Series(corr_groups).rename('context_hashtags').explode().to_frame()

            # add dates to contexts
            corr_groups['start_date'] = corr_groups['context_hashtags'].apply(
                lambda h_list: min([hashtag_peaks[hashtag_peaks['hashtag'] == h]['start_date'].values
                for h in h_list])[0])
            corr_groups['end_date'] = corr_groups['context_hashtags'].apply(
                lambda h_list: max([hashtag_peaks[hashtag_peaks['hashtag'] == h]['end_date'].values
                for h in h_list])[0])
            #corr_groups['start_date'] = corr_groups['start_date'].apply(lambda x: x.date())
            #corr_groups['end_date'] = corr_groups['end_date'].apply(lambda x: x.date())

            # rank candidate contexts
            ranked_users_hashtags = ranked_users_hashtags.explode('hashtags').rename(columns={'hashtags': 'hashtag'})
            ranked_users_hashtags['weight'] = \
                (ranked_users_hashtags['weight'] - ranked_users_hashtags['weight'].min()) / \
                (ranked_users_hashtags['weight'].max() - ranked_users_hashtags['weight'].min() * 9 + 1)

            corr_groups['hashtag_ranks'] = corr_groups['context_hashtags'].apply(
                lambda h_list: [ranked_users_hashtags[ranked_users_hashtags['hashtag'] == h]['weight'].tolist()
                for h in h_list])
            
            corr_groups['context_rank'] = corr_groups['hashtag_ranks'].apply(
                lambda r_list: sum([sum(r) / len(r) for r in r_list]) / len(r_list))
            corr_groups.sort_values(by='context_rank', inplace=True, ascending=False)
            print(corr_groups[['context_hashtags', 'context_rank']])

            corr_groups['context_rank_2'] = corr_groups['hashtag_ranks'].apply(
                lambda r_list: sum([len(r) for r in r_list]) / len(r_list))
            corr_groups.sort_values(by='context_rank_2', inplace=True, ascending=False)
            print(corr_groups[['context_hashtags', 'context_rank_2']])

            corr_groups['context_rank_3'] = corr_groups['hashtag_ranks'].apply(
                lambda r_list: sum([max(r) for r in r_list]) / len(r_list))
            corr_groups.sort_values(by='context_rank_3', inplace=True, ascending=False)
            print(corr_groups[['context_hashtags', 'context_rank_3']])

            corr_groups['context_rank_4'] = corr_groups['hashtag_ranks'].apply(
                lambda r_list: max([max(r) for r in r_list]))
            corr_groups.sort_values(by='context_rank_4', inplace=True, ascending=False)
            print(corr_groups[['context_hashtags', 'context_rank_4']])

            corr_groups['context_rank_5'] = corr_groups['hashtag_ranks'].apply(
                lambda r_list: sum([len(r) * max(r) for r in r_list]) / len(r_list))

            print(hashtag_peaks[hashtag_peaks['community'] == 1])

            self.datasources.files.write(corr_groups, 'context_detector', 'get_new_contexts', 'new_contexts', 'csv')