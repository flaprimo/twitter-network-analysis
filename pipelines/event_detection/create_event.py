import logging

from sqlalchemy.exc import IntegrityError
import helper
from datasources import PipelineIO
from datasources.database.database import db
from datasources.database.model import Event
from datasources.tw.tw import tw

logger = logging.getLogger(__name__)


class CreateEvent:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input(['event'], stage_input, stage_input_format)
        self.output_prefix = 'ce'
        self.output_format = {
            'event': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'event'),
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
                },
                'w_kwargs': {}
            },
            'stream': {
                'type': 'json',
                'path': self.config.get_path(self.output_prefix, 'stream', 'json'),
                'r_kwargs': {},
                'w_kwargs': {}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.skip_output_check or not self.output:
            self.output['event'] = self.input['event']
            self.output['tw'] = self.__harvest_event(self.input['event'])

            if self.config.save_db_output:
                self.__persist_event(self.input['event'])

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __persist_event(event):
        logger.info('persist event')
        event_record = event.reset_index().to_dict('records')[0]
        event_record['hashtags'] = ' '.join(event_record['hashtags'])
        event_entity = Event(**event_record)

        try:
            with db.session_scope() as session:
                session.add(event_entity)
            logger.debug('event successfully persisted')
        except IntegrityError:
            logger.debug('event already exists or constraint is violated and could not be added')

    @staticmethod
    def __harvest_event(event):
        logger.info('harvest event')
        logger.debug('dropped columns:\n' + helper.df_tostring(event))

        event_record = event.to_dict('records')[0]

        search = tw.tw_api.create_search(query=' OR '.join(event_record['hashtags']),
                                         since=event_record['start_date'],
                                         until=event_record['end_date'],
                                         n=100)
        tw_list = [tw_json._json for tw_json in search]

        return tw_list
