import os
import pandas as pd
from datetime import datetime


class Contexts:
    def __init__(self, input_path):
        self.input_contexts_path = os.path.join(input_path, 'contexts/contexts.csv')
        self.contexts = Contexts.__import_contexts(self.input_contexts_path)

    @staticmethod
    def __import_contexts(input_contexts_path):
        events = pd.read_csv(input_contexts_path,
                             dtype={
                                 'name': str,
                                 'start_date': str,
                                 'end_date': str,
                                 'location': str
                             },
                             converters={'hashtags': lambda x: x.split(' ')},
                             parse_dates=['start_date', 'end_date'], index_col='name',
                             date_parser=lambda x: datetime.strptime(x, '%Y-%m-%d'))
        events['start_date'] = events['start_date'].apply(lambda x: x.date())
        events['end_date'] = events['end_date'].apply(lambda x: x.date())

        return events

    def get_context(self, context_name):
        return self.contexts[self.contexts.index == context_name]

    def get_context_names(self):
        return self.contexts.index.tolist()
