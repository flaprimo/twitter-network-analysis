import pandas as pd
import networkx as nx
import logging
import helper

logger = logging.getLogger(__name__)


class PipelineIO:
    @staticmethod
    def __read_files(io_format):
        def read_pandas(path, kwargs):
            return pd.read_csv(path, **kwargs)

        def read_networkx(path, kwargs):
            graph = nx.read_gexf(path, **kwargs)
            for n in graph.nodes(data=True):
                n[1].pop('label', None)
            return graph

        io_values = {}
        for o_name, o_format in io_format.items():
            if o_format['type'] == 'pandas':
                io_values[o_name] = read_pandas(o_format['path'], o_format['r_kwargs'])

            elif o_format['type'] == 'networkx':
                io_values[o_name] = read_networkx(o_format['path'], o_format['r_kwargs'])

            else:
                raise ValueError('error: unknown file type')

        return io_values

    @staticmethod
    def __write_files(io_format, io_values):
        def write_pandas(df, path, kwargs):
            df.to_csv(path, **kwargs)

        def write_networkx(graph, path, kwargs):
            nx.write_gexf(graph, path, **kwargs)

        debug_output = ''
        for o_name, o_value in io_values.items():
            o_format = io_format[o_name]

            if o_format['type'] == 'pandas':
                write_pandas(o_value, o_format['path'], o_format['w_kwargs'])
                o_debug = helper.df_tostring(o_value, 5)

            elif o_format['type'] == 'networkx':
                write_networkx(o_value, o_format['path'], o_format['w_kwargs'])
                o_debug = helper.graph_tostring(o_value, 3, 3)

            else:
                raise ValueError('error: unknown file type')

            debug_output += f'{o_name} file path: {o_format}\n' + o_debug

        return debug_output

    @staticmethod
    def load_input(stage_input_expected, stage_input, input_format):
        logger.info('load input')
        if stage_input is not None and \
                isinstance(stage_input, dict) and \
                all(i in stage_input for i in stage_input_expected):
            logger.debug(f'input present')
            return stage_input

        else:
            logger.debug(f'input not present, loading input')
            return PipelineIO.__read_files(input_format)

    @staticmethod
    def load_output(output_format):
        logger.info('load output')
        try:
            output = PipelineIO.__read_files(output_format)
            logger.debug(f'output present, not executing stage')
            return output

        except IOError as e:
            logger.debug(f'output not present, executing stage: {e}')
            return {}

    @staticmethod
    def save_output(output, output_format):
        logger.info('save output')
        logger.debug(PipelineIO.__write_files(output_format, output))
