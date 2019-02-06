import logging
from sqlalchemy.exc import IntegrityError
from datasources.database.model import Event
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
                    }
                }
            }
        ]
        tasks = [self.__create_context, self.__harvest_context]
        super(ContextDetection, self).__init__('context_detection', files, tasks, datasources, file_prefix)

    def __create_context(self):
        if not self.datasources.files.exists(
                'context_detection', 'create_context', 'context', 'csv', self.context_name):
            logger.info('persist context')
            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.reset_index().to_dict('records')[0]
            context_record['hashtags'] = ' '.join(context_record['hashtags'])

            try:
                with self.datasources.database.session_scope() as session:
                    context_entity = Event(**context_record)
                    session.add(context_entity)
                logger.debug('context successfully persisted')
            except IntegrityError:
                logger.debug('context already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                context, 'context_detection', 'create_context', 'context', 'csv', self.context_name)

    def __harvest_context(self):
        self.datasources.files.add_file_model(
            pipeline_name='context_detection',
            stage_name='harvest_context',
            file_name='stream',
            file_extension='json',
            file_prefix=self.context_name)
        # if not self.datasources.files.exists(
        #         'context_detection', 'harvest_context', 'stream', 'json', self.context_name):
        #     context = self.datasources.contexts.get_context(self.context_name)
        #     context_record = context.to_dict('records')[0]
        #
        #     stream = tw.tw_premium_api.create_search(query=' OR '.join(context_record['hashtags']),
        #                                              since=context_record['start_date'],
        #                                              until=context_record['end_date'],
        #                                              n=200)
        #
        #     self.datasources.files.write_file(stream, 'context_detection', 'harvest_context', 'stream', 'json')
