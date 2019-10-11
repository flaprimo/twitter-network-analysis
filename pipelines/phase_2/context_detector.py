import logging
import pandas as pd
from datetime import datetime

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
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d')
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
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d')
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
                        'hashtag': str,
                        'start_date': str,
                        'end_date': str
                    },
                    'parse_dates': ['start_date', 'end_date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__hashtags_frequency, self.__find_peaks]
        super(ContextDetector, self).__init__('context_detector', files, tasks, datasources)

    def __hashtags_frequency(self):
        if not self.datasources.files.exists('context_detector', 'hashtags_frequency', 'hashtags_frequency', 'csv'):
            tweets = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')[['date', 'hashtags']]\
                .explode('hashtags').dropna().rename(columns={'hashtags': 'hashtag'})
            tweets = tweets.groupby('hashtag').resample('D', on='date').size().to_frame('count')

            # subtract mean and retain only counts greater than 0
            tweets = tweets.groupby('hashtag').transform(lambda x: x - x[x > 0].mean()).reset_index()
            tweets = tweets[tweets['count'] > 0]

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
                peaks = find_peaks(timeline, height=timeline[timeline > 0].quantile(.9))[0]

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

            hashtag_peaks = pd.DataFrame(hashtag_peaks)\
                .sort_values(['hashtag', 'peak_value', 'peak_width'], ascending=[True, False, False])\
                .drop_duplicates(['hashtag', 'start_date', 'end_date'], keep='first').reset_index(drop=True)

            self.datasources.files.write(
                hashtag_peaks, 'context_detector', 'find_peaks', 'hashtags_peaks', 'csv')

            # find peaks
            # https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.peak_widths.html#scipy.signal.peak_widths
