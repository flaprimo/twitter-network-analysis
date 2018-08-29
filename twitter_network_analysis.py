import logging
from stages import PreProcessing, CommunityDetection, Metrics
from config import Config

log_format = '%(name)s - %(message)s'
logging.basicConfig(format=log_format)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)


def main():
    root_logger.info('Starting Twitter Network Analysis')

    # demon config
    epsilon = [0.25, 0.5, 0.75]
    min_community_size = 3

    pre_processing = PreProcessing(Config())
    pre_processing.execute()
    pre_processing.save()

    edges = pre_processing.edges

    for e in epsilon:
        config = Config(
            demon={
                'epsilon': e,
                'min_community_size': min_community_size
            })
        community_detection = CommunityDetection(config, edges)
        community_detection.execute()
        community_detection.save()

        nodes = community_detection.nodes
        edges = community_detection.edges

        metrics = Metrics(config, nodes, edges)
        metrics.execute()
        metrics.save()

        metrics.metric_top('hindex')
        metrics.metric_top('indegree')


if __name__ == '__main__':
    main()
