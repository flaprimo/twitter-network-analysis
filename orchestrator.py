import os
import logging
import time
from datasources import Datasources
from pipelines.phase_1 import ContextHarvesting, NetworkCreation, NetworkMetrics, CommunityDetection, \
    CommunityDetectionMetrics, ProfileMetrics, UserContextMetrics, Persistence
from pipelines.phase_2 import Ranking, UserTimelines, ContextDetector, BipartiteGraph, BipartiteCommunityDetection

logging.basicConfig(level=logging.DEBUG, filename='logs/debug.log',
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, project_name, input_path, output_path):
        if not os.path.isdir(os.path.join(input_path, project_name)):
            raise FileNotFoundError(f'project {project_name} doesn\'t exist')

        self.project_name = project_name
        self.project_input_path = os.path.join(input_path, project_name)
        self.project_output_path = os.path.join(output_path, project_name)
        self.datasources = Datasources(self.project_input_path, self.project_output_path)

        logger.info('INIT Orchestrator')

    def execute(self):
        start_time = time.time()
        logger.info('START Orchestrator')

        pipeline_1 = [ContextHarvesting, NetworkCreation, NetworkMetrics, CommunityDetection, CommunityDetectionMetrics,
                      ProfileMetrics, UserContextMetrics, Persistence]
        for context_name in self.datasources.contexts.get_context_names():
            logger.info(f'EXEC pipeline for {context_name}')
            for p in pipeline_1:
                current_pipeline = p(self.datasources, context_name)
                current_pipeline.execute()

        pipeline_2 = [Ranking, UserTimelines, BipartiteGraph, BipartiteCommunityDetection, ContextDetector]  #, HashtagsVector] #, HashtagsNetwork]
        for p in pipeline_2:
            current_pipeline = p(self.datasources)
            current_pipeline.execute()

        logger.info('END Orchestrator')
        logger.debug(f'elapsed time: {round(time.time() - start_time, 4)} s')
