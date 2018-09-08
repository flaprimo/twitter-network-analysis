import pandas as pd
import logging
import helper

logger = logging.getLogger(__name__)


class PreProcessing:
    def __init__(self, config):
        self.config = config
        logger.info(f'PRE-PROCESSING: {self.config.data_filename}')
        self.edges = pd.read_csv(self.config.data_path,
                                 dtype=self.config.data_type['csv_data'])

    def execute(self):
        logger.info(f'execute')
        self.__drop_columns()
        self.__merge_duplicates()

    def __drop_columns(self):
        columns_todrop = ['cod', 'user_from_fav_count', 'user_rt_fav_count', 'text']
        self.edges = self.edges.drop(columns=columns_todrop, axis=1)
        self.edges.rename(columns={'user_from_name': 'Source', 'user_to_name': 'Target', 'weights': 'Weight'},
                          inplace=True)

        logger.info('drop columns')
        logger.debug(f'dropped columns: {columns_todrop}\n' +
                     helper.df_tostring(self.edges, 5))

    def __merge_duplicates(self):
        self.edges.Source = self.edges.Source.str.lower()
        self.edges.Target = self.edges.Target.str.lower()

        df_edges_duplicates = self.edges[self.edges.duplicated(subset=['Source', 'Target'], keep='first')]
        self.edges = self.edges.groupby(['Source', 'Target']).sum().reset_index()

        logger.info('merge duplicates columns')
        logger.debug(f'number of duplicates: {df_edges_duplicates.shape}\n' +
                     helper.df_tostring(self.edges, 5))

    def save(self):
        edges_path = self.config.get_path('pp', 'edges', has_postfix=False)

        self.edges.to_csv(edges_path, index=False)

        logger.info('save csv')
        logger.debug(f'edges file path: {edges_path}\n' +
                     helper.df_tostring(self.edges, 5))
