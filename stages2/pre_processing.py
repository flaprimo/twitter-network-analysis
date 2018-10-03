import pandas as pd
import logging
import helper

logger = logging.getLogger(__name__)


class PreProcessing:
    def __init__(self, config, stage_input=None):
        self.config = config
        self.stage_prefix = 'pp'
        self.input = self.__load_input(stage_input)
        self.output = self.__load_output()
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['edges'] = self.__drop_columns(self.input['data'])
            self.output['edges'] = self.__merge_duplicates(self.output['edges'])
            self.output['nodes'] = self.__get_nodes(self.output['edges'])
            self.output['edges'] = self.__rename_edges(self.output['nodes'], self.output['edges'])

            self.__save_output()

        logger.info(f'END for {self.config.data_filename}')

        return self.output

    def __load_input(self, stage_input):
        logger.info('load input')
        if helper.check_input(['data'], stage_input):
            logger.debug(f'input present')
            return stage_input
        else:
            logger.debug(f'input not present, loading input')
            return {
                'data': pd.read_csv(self.config.data_path,
                                    dtype=self.config.data_type['csv_data'])
            }

    def __load_output(self):
        logger.info('load output')
        try:
            output = {
                'edges': pd.read_csv(self.config.get_path(self.stage_prefix, 'edges'),
                                     dtype=self.config.data_type['csv_edges']),
                'nodes': pd.read_csv(self.config.get_path(self.stage_prefix, 'nodes'),
                                     dtype=self.config.data_type['csv_nodes'], index_col=0)
            }
            logger.debug(f'output present, not executing stage')

            return output
        except IOError as e:
            logger.debug(f'output not present, executing stage: {e}')

            return {}

    def __save_output(self):
        edges_path = self.config.get_path(self.stage_prefix, 'edges')
        nodes_path = self.config.get_path(self.stage_prefix, 'nodes')

        self.output['edges'].to_csv(edges_path, index=False)
        self.output['nodes'].to_csv(nodes_path)

        logger.info('save output')
        logger.debug(f'edges file path: {edges_path}\n' +
                     helper.df_tostring(self.output['edges'], 5) +
                     f'nodes file path: {nodes_path}\n' +
                     helper.df_tostring(self.output['nodes'], 5))

    @staticmethod
    def __drop_columns(edges):
        columns_tokeep = ['user_from_name', 'user_to_name', 'weights']
        columns_todrop = [c for c in edges.columns.values if c not in columns_tokeep]
        edges = edges[columns_tokeep]
        edges = edges.rename(columns={'user_from_name': 'Source', 'user_to_name': 'Target', 'weights': 'Weight'})

        logger.info('drop columns')
        logger.debug(f'dropped columns: {columns_todrop}\n' +
                     helper.df_tostring(edges, 5))

        return edges

    @staticmethod
    def __merge_duplicates(edges):
        edges.Source = edges.Source.str.lower()
        edges.Target = edges.Target.str.lower()

        df_edges_duplicates = edges[edges.duplicated(subset=['Source', 'Target'], keep='first')]
        edges = edges.groupby(['Source', 'Target']).sum().reset_index()

        logger.info('merge duplicates columns')
        logger.debug(f'number of duplicates: {df_edges_duplicates.shape}\n' +
                     helper.df_tostring(edges, 5))

        return edges

    @staticmethod
    def __get_nodes(edges):
        nodes = pd.concat([edges.Source, edges.Target], axis=0).drop_duplicates() \
            .reset_index(drop=True).to_frame('Username')

        logger.info('get nodes')
        logger.debug(f'nodes: {nodes.shape}\n' +
                     helper.df_tostring(nodes, 5))

        return nodes

    @staticmethod
    def __rename_edges(nodes, edges):
        nodes_dict = {v: k for k, v in nodes.to_dict()['Username'].items()}
        edges.Source = edges.Source.map(nodes_dict.get)
        edges.Target = edges.Target.map(nodes_dict.get)

        logger.info('rename edges')
        logger.debug(f'renamed edges: {edges.shape}\n' +
                     helper.df_tostring(edges, 5))

        return edges
