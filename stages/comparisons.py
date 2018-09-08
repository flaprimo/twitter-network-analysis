from stages import Metrics, Analysis
import pandas as pd
import helper
import logging

logger = logging.getLogger(__name__)


class Comparisons:
    @staticmethod
    def get_comparison_top_community(m):
        topc_path = m.config.comparison
        topc = pd.read_csv(topc_path, index_col='TOP')

        logger.info('get comparison top community')
        logger.debug(f'top communities file path: {topc_path}\n' +
                     helper.df_tostring(topc))

        return pd.read_csv(m.config.comparison, index_col='TOP')

    @staticmethod
    def communities_top_users_heatmap(m, other, metric_name):
        logger.info(f'{metric_name} heatmap comparison')
        for e in list(m.keys()):
            logger.debug(f'{metric_name} for DEMON e={e} vs other algorithm')
            p1 = Metrics.metric_top(m[e].metric_top_values(metric_name))

            Analysis.show_heatmap(p1, other, f'DEMON e={e}', 'other algorithm', metric_name, False)

    @staticmethod
    def communities_top_users_rank(m, other, metric_name, threshold=0.5):
        logger.info(f'{metric_name} heatmap')

        r_list = []
        for e in list(m.keys()):
            logger.debug(f'{metric_name} for DEMON e={e} vs other algorithm')
            p1 = Metrics.metric_top(m[e].metric_top_values(metric_name))

            r_list.append(Analysis.top_rank(p1, other, f'DEMON e={e}', 'other algorithm',
                                            metric_name, threshold, False))

        return r_list
