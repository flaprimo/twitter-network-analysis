import pandas as pd
import networkx as nx
import logging
import helper
import infomap

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self, config, stage_input=None):
        self.config = config
        self.prev_stage_prefix = 'cd'
        self.stage_prefix = 'm'
        self.input = self.__load_input(stage_input)
        self.output = self.__load_output()
        logger.info(f'INIT for {self.config.data_filename}')

    def execute(self):
        logger.info(f'EXEC for {self.config.data_filename}')

        if not self.output:
            self.output['graph'] = self.__get_graph(self.input['edges'], self.input['nodes'])
            self.output['graph'] = self.__find_communities(self.output['graph'])
            self.output['nodes'] = self.__add_community_to_nodes(self.output['graph'], self.input['nodes'])
            self.output['graph'], self.output['nodes'], self.output['edges'] = \
                self.__remove_lone_nodes(self.output['graph'], self.input['nodes'], self.input['edges'])
            self.__save_output()

        logger.info(f'END for {self.config.data_filename}')

        return self.output

    def __load_input(self, stage_input):
        logger.info('load input')
        if helper.check_input(['edges', 'nodes'], stage_input):
            logger.debug(f'input present')
            return stage_input
        else:
            logger.debug(f'input not present, loading input')
            return {
                'edges': pd.read_csv(self.config.get_path(self.prev_stage_prefix, 'edges'),
                                     dtype=self.config.data_type['csv_edges']),
                'nodes': pd.read_csv(self.config.get_path(self.prev_stage_prefix, 'nodes'),
                                     dtype=self.config.data_type['csv_nodes'], index_col=0),
            }

    def __load_output(self):
        logger.info('load output')
        try:
            output = {
                'graph': nx.read_gexf(self.config.get_path(self.stage_prefix, 'graph', 'gexf')),
                'nodes': pd.read_csv(self.config.get_path(self.stage_prefix, 'nodes'),
                                     dtype=self.config.data_type['csv_nodes']),
                'edges': pd.read_csv(self.config.get_path(self.stage_prefix, 'edges'),
                                     dtype=self.config.data_type['csv_nodes'])
            }
            logger.debug(f'output present, not executing stage')

            return output
        except IOError as e:
            logger.debug(f'output not present, executing stage: {e}')

            return {}

    def __save_output(self):
        graph_path = self.config.get_path(self.stage_prefix, 'graph', 'gexf')
        nodes_path = self.config.get_path(self.stage_prefix, 'nodes')
        edges_path = self.config.get_path(self.stage_prefix, 'edges')

        nx.write_gexf(self.output['graph'], graph_path)
        self.output['nodes'].to_csv(nodes_path)
        self.output['edges'].to_csv(edges_path, index=False)

        logger.info('save output')
        logger.debug(f'nodes file path: {nodes_path}\n' +
                     helper.df_tostring(self.output['nodes'], 5) +
                     f'edges file path: {nodes_path}\n' +
                     helper.df_tostring(self.output['edges'], 5) +
                     f'graph file path: {graph_path}\n' +
                     helper.graph_tostring(self.output['graph'], 3, 3))