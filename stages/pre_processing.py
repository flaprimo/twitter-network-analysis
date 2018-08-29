import pandas as pd
import logging
import helper

logger = logging.getLogger(__name__)


class PreProcessing:
    def __init__(self, config):
        self.config = config
        self.edges = pd.read_csv(self.config.data_path,
                                 dtype=self.config.data_type['csv_data'])

    def execute(self):
        logging.info('PRE-PROCESSING')
        self.__drop_columns()
        self.__merge_duplicates()

    def __drop_columns(self):
        self.edges = self.edges.drop(columns=['cod', 'user_from_fav_count', 'user_rt_fav_count', 'text'], axis=1)
        self.edges.rename(columns={'user_from_name': 'Source', 'user_to_name': 'Target', 'weights': 'Weight'},
                          inplace=True)

        logger.info('drop columns\n' +
                    helper.df_tostring(self.edges))

    def __merge_duplicates(self):
        self.edges.Source = self.edges.Source.str.lower()
        self.edges.Target = self.edges.Target.str.lower()

        df_edges_duplicates = self.edges[self.edges.duplicated(subset=['Source', 'Target'], keep='first')]
        self.edges = self.edges.groupby(['Source', 'Target']).sum().reset_index()

        logger.info('merge duplicates\n'
                    f'  number of duplicates: {df_edges_duplicates.shape}\n' +
                    helper.df_tostring(self.edges))

    def save(self):
        self.edges.to_csv(self.config.get_path('pp', 'edges'), index=False)

        logger.info('save csv\n'
                    f'  path: {self.config.get_path("pp", "edges")}\n' +
                    helper.df_tostring(self.edges))
