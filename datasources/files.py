import os
import networkx as nx
import pandas as pd
import json
import logging
from cachetools import LRUCache

logger = logging.getLogger(__name__)


class Files:
    def __init__(self, output_path):
        self.output_path = os.path.join(output_path, 'files')
        self.model = {}
        self.cache = LRUCache(maxsize=30)

    @staticmethod
    def __get_full_file_name(file_name, file_extension, file_prefix='', file_suffix=''):
        file_prefix = file_prefix+'__' if file_prefix else ''
        file_suffix = file_suffix+'__' if file_suffix else ''

        return f'{file_prefix}{file_name}{file_suffix}.{file_extension}'

    def exists(self, pipeline_name, stage_name, file_name, file_extension, file_prefix='', file_suffix=''):
        full_file_name = Files.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)
        file_model = self.model[pipeline_name][stage_name][full_file_name]
        file_exists = os.path.isfile(file_model['path'])
        if file_exists:
            logger.debug(f'file exists (file "{file_model["path"]}")')
        else:
            logger.debug(f'file NOT exists (file "{file_model["path"]}")')

        return file_exists

    def add_file_models(self, file_model_list):
        for file_model in file_model_list:
            self.add_file_model(**file_model)

    def add_file_model(self, pipeline_name, stage_name, file_name, file_extension,
                       file_prefix='', file_suffix='', r_kwargs=None, w_kwargs=None):
        path_dir = os.path.join(self.output_path, f'{pipeline_name}/{stage_name}')
        full_file_name = Files.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)

        new_file = {
            full_file_name: {
                'path': os.path.join(path_dir, full_file_name),
                'path_dir': path_dir,
                'type': file_extension,
                'r_kwargs': r_kwargs if r_kwargs else {},
                'w_kwargs': w_kwargs if w_kwargs else {}
            }
        }

        if pipeline_name not in self.model:
            self.model[pipeline_name] = {}
        if stage_name not in self.model[pipeline_name]:
            self.model[pipeline_name][stage_name] = {}

        self.model[pipeline_name][stage_name].update(new_file)

        logger.debug(f'added file model (file "{new_file[full_file_name]["path"]}")')

    def read(self, pipeline_name, stage_name, file_name, file_extension, file_prefix='', file_suffix=''):
        def read_pandas(file_path, kwargs):
            return pd.read_csv(file_path, **kwargs)

        def read_json(file_path, kwargs):
            with open(file_path) as json_file:
                json_content = json.load(json_file, **kwargs)
            return json_content

        def read_networkx(path, kwargs):
            graph = nx.read_gexf(path, **kwargs)
            for n in graph.nodes(data=True):
                n[1].pop('label', None)
            return graph

        full_file_name = Files.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)

        try:
            file_model = self.model[pipeline_name][stage_name][full_file_name]

            try:
                # raise KeyError
                m = self.cache[file_model['path']]
                logger.info('loading from cache')
                logger.debug(f'file read from cache (file "{file_model["path"]}")')
                return m.copy()
            except KeyError:
                if file_model['type'] == 'csv':
                    file_content = read_pandas(file_model['path'], file_model['r_kwargs'])
                elif file_model['type'] == 'json':
                    file_content = read_json(file_model['path'], file_model['r_kwargs'])
                elif file_model['type'] == 'gexf':
                    file_content = read_networkx(file_model['path'], file_model['r_kwargs'])
                else:
                    raise ValueError('error: unknown file type')

                logger.debug(f'file read (file "{file_model["path"]}")')
                # self.cache[file_model['path']] = file_content

                return file_content
        except KeyError:
            return None

    def write(self, file_content,
              pipeline_name, stage_name, file_name, file_extension, file_prefix='', file_suffix=''):
        def write_pandas(df, file_path, kwargs):
            df.to_csv(file_path, **kwargs)

        def write_json(json_content, file_path, kwargs):
            with open(file_path, 'w') as json_file:
                json.dump(json_content, json_file, **kwargs)

        def write_networkx(graph, path, kwargs):
            nx.write_gexf(graph, path, **kwargs)

        full_file_name = Files.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)
        file_model = self.model[pipeline_name][stage_name][full_file_name]

        if not os.path.exists(file_model['path_dir']):
            os.makedirs(file_model['path_dir'])

        if file_model['type'] == 'csv':
            write_pandas(file_content, file_model['path'], file_model['w_kwargs'])
        elif file_model['type'] == 'json':
            write_json(file_content, file_model['path'], file_model['w_kwargs'])
        elif file_model['type'] == 'gexf':
            write_networkx(file_content, file_model['path'], file_model['w_kwargs'])
        else:
            raise ValueError('error: unknown file type')

        logger.debug(f'file written (file "{file_model["path"]}")')
        self.cache[file_model['path']] = file_content
