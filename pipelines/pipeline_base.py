import logging
from concurrent.futures import ThreadPoolExecutor
import time

logger = logging.getLogger(__name__)


class PipelineBase:
    def __init__(self, pipeline_name, files, tasks, datasources, context_name, retries=3):
        self.pipeline_name = pipeline_name
        self.tasks = tasks
        self.datasources = datasources
        self.context_name = context_name
        self.retries = retries

        files = [dict(f, **{'pipeline_name': pipeline_name, 'file_prefix': context_name}) for f in files]
        self.datasources.files.add_file_models(files)

        logger.info(f'INIT PIPELINE {pipeline_name} for {context_name}')

    def execute(self):
        logger.info(f'START PIPELINE {self.pipeline_name} for {self.context_name}')

        for task in self.tasks:
            if isinstance(task, list):
                logger.debug(f'parallel execution of [{", ".join(t.__name__ for t in task)}] for {self.context_name}')
                with ThreadPoolExecutor() as executor:
                    for t in task:
                        executor.submit(t)
            else:
                self.__task_execution(task)

        logger.info(f'END PIPELINE {self.pipeline_name} for {self.context_name}')

    def __task_execution(self, task):
        logger.info(f'START TASK {task.__name__} for {self.context_name}')

        is_executed = False
        retries = 0
        while not is_executed and retries < self.retries:
            try:
                task()
                is_executed = True
                logger.debug(f'SUCCESSFUL TASK {task.__name__} for {self.context_name}')
            except Exception as e:
                logger.error(f'ERROR TASK {task.__name__} for {self.context_name}: {e}')
                retries += 1
                time.sleep(retries)

        logger.info(f'END TASK {task.__name__} for {self.context_name}')
