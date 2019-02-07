import os
import logging
import time
from datasources import Datasources
from pipelines import NetworkCreation, ContextDetection, NetworkMetrics, CommunityDetection, \
    CommunityDetectionMetrics, ProfileMetrics, UserContextMetrics

logging.basicConfig(level=logging.DEBUG, filename='logs/debug.log',
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, project_name, input_path, output_path):
        self.project_name = project_name
        self.project_input_path = os.path.join(input_path, project_name)
        self.project_output_path = os.path.join(output_path, project_name)
        self.datasources = Datasources(self.project_input_path, self.project_output_path)

        logger.info('INIT Orchestrator')

    def execute(self):
        start_time = time.time()
        logger.info('EXEC Orchestrator')

        for context_name in self.datasources.contexts.get_context_names():
            context_detection = ContextDetection(self.datasources, context_name)
            context_detection.execute()

            network_creation = NetworkCreation(self.datasources, context_name)
            network_creation.execute()

            network_metrics = NetworkMetrics(self.datasources, context_name)
            network_metrics.execute()

            community_detection = CommunityDetection(self.datasources, context_name)
            community_detection.execute()

            community_detection_metrics = CommunityDetectionMetrics(self.datasources, context_name)
            community_detection_metrics.execute()

            profile_metrics = ProfileMetrics(self.datasources, context_name)
            profile_metrics.execute()

            usercontext_metrics = UserContextMetrics(self.datasources, context_name)
            usercontext_metrics.execute()

        logger.info('END Orchestrator')
        logger.debug(f'elapsed time: {round(time.time() - start_time, 4)} s')
