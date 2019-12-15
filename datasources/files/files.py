import json
import os
import logging
from cachetools import LRUCache
from .model import file_models

logger = logging.getLogger(__name__)


class Files:
    def __init__(self, output_path):
        self.output_path = os.path.join(output_path, 'files')
        self.model = {}
        self.cache = LRUCache(maxsize=0)

    @staticmethod
    def __get_full_file_name(file_name, file_extension, file_prefix='', file_suffix=''):
        file_prefix = file_prefix + '__' if file_prefix else ''
        file_suffix = file_suffix + '__' if file_suffix else ''

        return f'{file_prefix}{file_name}{file_suffix}.{file_extension}'

    def exists(self, pipeline_name, stage_name, file_name, file_extension, file_prefix='', file_suffix=''):
        full_file_name = self.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)
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
        self.output_file_models()

    def add_file_model(self, pipeline_name, stage_name, file_name, file_extension,
                       file_prefix='', file_suffix='', r_kwargs=None, w_kwargs=None):
        path_dir = os.path.join(self.output_path, f'{pipeline_name}/{stage_name}')
        full_file_name = self.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)

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
        full_file_name = self.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)

        try:
            file_model = self.model[pipeline_name][stage_name][full_file_name]

            try:
                m = self.cache[file_model['path']]
                logger.debug(f'file read from cache (file "{file_model["path"]}")')
                return m.copy()
            except KeyError:
                file_driver = file_models.get(file_model['type'])

                if file_driver:
                    file_content = file_driver.reader(file_model['path'], file_model['r_kwargs'])
                else:
                    raise KeyError('error: unknown file type')

                logger.debug(f'file read (file "{file_model["path"]}")')

                return file_content
        except KeyError:
            return None

    def write(self, file_content,
              pipeline_name, stage_name, file_name, file_extension, file_prefix='', file_suffix=''):
        full_file_name = self.__get_full_file_name(file_name, file_extension, file_prefix, file_suffix)
        file_model = self.model[pipeline_name][stage_name][full_file_name]

        if not os.path.exists(file_model['path_dir']):
            os.makedirs(file_model['path_dir'])

        file_driver = file_models.get(file_model['type'])

        if file_driver:
            file_preview = file_driver.writer(file_content, file_model['path'], file_model['w_kwargs'])
        else:
            raise KeyError('error: unknown file type')

        logger.debug(f'file written (file "{file_model["path"]}")\n' + str(file_preview))
        # self.cache[file_model['path']] = file_content

    def output_file_models(self):
        files_model_path = os.path.join(self.output_path, 'files_model.json')
        with open(files_model_path, 'w') as json_file:
            json.dump(self.model, json_file)
