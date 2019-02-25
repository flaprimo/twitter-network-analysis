from datetime import datetime
import logging
from datasources.tw.tw import tw
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class ContextDetection(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name': 'create_context',
                'file_name': 'context',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'name': str,
                        'start_date': str,
                        'end_date': str,
                        'location': str
                    },
                    'converters': {
                        'hashtags': lambda x: x.strip('[]').replace('\'', '').split(', ')
                    },
                    'parse_dates': ['start_date', 'end_date'],
                    'date_parser': lambda x: datetime.strptime(x, '%Y-%m-%d'),
                    'index_col': 'name'
                }
            },
            {
                'stage_name': 'harvest_context',
                'file_name': 'stream',
                'file_extension': 'json'
            }
        ]
        tasks = [self.__create_context]  # [[self.__create_context, self.__harvest_context]]
        super(ContextDetection, self).__init__('context_detection', files, tasks, datasources, file_prefix)

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

            self.datasources.files.write_file(stream, 'context_detection', 'harvest_context', 'stream', 'json')
