import logging
from datasources import tw
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class UserTimelines(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_user_timelines',
                'file_name': 'stream',
                'file_extension': 'json',
                'r_kwargs': {
                    'dtype': {
                        'id': 'uint32',
                        'user_name': str
                    },
                    'index_col': 'id'
                }
            }
        ]
        tasks = [self.__get_user_timelines]
        super(UserTimelines, self).__init__('user_timelines', files, tasks, datasources)

    def __get_user_timelines(self):
        if not self.datasources.files.exists(
                'user_timelines', 'get_user_timelines', 'stream', 'json'):
            rank_2 = self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')['user_name'].head(100).tolist()

            stream = tw.tw_api.get_user_timelines(rank_2, 50)

            self.datasources.files.write(stream, 'user_timelines', 'get_user_timelines', 'stream', 'json')
