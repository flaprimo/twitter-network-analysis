import logging

logger = logging.getLogger(__name__)


class PipelineBase:
    def __init__(self, pipeline_name, files, tasks, datasources, context_name):
        self.pipeline_name = pipeline_name
        self.tasks = tasks
        self.datasources = datasources
        self.context_name = context_name

        files = [dict(f, **{'pipeline_name': pipeline_name, 'file_prefix': context_name}) for f in files]
        self.datasources.files.add_file_models(files)

        logger.info(f'INIT PIPELINE {pipeline_name} for {context_name}')

    def execute(self):
        logger.info(f'EXEC PIPELINE {self.pipeline_name} for {self.context_name}')

        for task in self.tasks:
            logger.info(f'EXEC TASK {task.__name__} for {self.context_name}')
            task()
            logger.info(f'END TASK {task.__name__} for {self.context_name}')

        logger.info(f'END PIPELINE {self.pipeline_name} for {self.context_name}')
