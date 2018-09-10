from stages import Metrics
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class Analysis:
    @staticmethod
    def communities_top_users_heatmap(m, metric_name, e_iterator, triangular=False):
        logger.info(f'{metric_name} heatmap')
        for x, y in e_iterator:
            logger.debug(f'{metric_name} for e={x} vs {y}')
            p1 = Metrics.metric_top(m[x].metric_top_values(metric_name))
            p2 = Metrics.metric_top(m[y].metric_top_values(metric_name))

            Analysis.show_heatmap(p1, p2, f'e={x}', f'e={y}', metric_name, triangular)

    @staticmethod
    def show_heatmap(p1, p2, p1_label, p2_label, metric_name, triangular):
        hm = Metrics.compare_metric_top(p1, p2)[0]

        # Generate a mask for the upper triangle
        if triangular:
            mask = np.zeros_like(hm, dtype=np.bool)
            mask[np.triu_indices_from(mask)] = True
        else:
            mask = None

        plt.figure(figsize=(15, 15))
        ax = sns.heatmap(hm, mask=mask, annot=True, cbar=False)\
            .set_title(f'{metric_name} for {p1_label} vs {p2_label}')
        plt.xlabel(p2_label)
        plt.ylabel(p1_label)
        plt.show()

    @staticmethod
    def communities_top_users_rank(m, metric_name, e_iterator, threshold=0.5):
        logger.info(f'{metric_name} heatmap')

        r_list = []
        for x, y in e_iterator:
            logger.debug(f'{metric_name} for e={x} vs {y}')
            p1 = Metrics.metric_top(m[x].metric_top_values(metric_name))
            p2 = Metrics.metric_top(m[y].metric_top_values(metric_name))

            p1_label = f'p1_e{x}'
            p2_label = f'p2_e{y}'
            same_partition = x == y

            r_list.append(Analysis.top_rank(p1, p2, p1_label, p2_label, metric_name, threshold, same_partition))

        return r_list

    @staticmethod
    def top_rank(p1, p2, p1_label, p2_label, metric_name, threshold, same_partition):
        r = Metrics.compare_metric_top(p1, p2)[1]\
            .to_frame().reset_index(col_fill=['A'])\
            .rename({'level_0': p1_label, 'level_1': p2_label, 0: metric_name}, axis=1)

        # FILTERING
        # remove same community comparisons if same epsilon
        r = r[r[p1_label] != r[p2_label]].reset_index(drop=True) if same_partition else r

        # remove values below threshold
        r = r[r[metric_name] >= threshold]

        # remove duplicates with switched labels
        r = r[(r[p1_label].isin(r[p2_label]) & r[p2_label].isin(r[p1_label])) &
              (r[p1_label] < r[p2_label])].reset_index(drop=True)

        # SORTING
        r['index_p1'] = r[p1_label].map(lambda i: int(i.split("_")[1]))
        r['index_p2'] = r[p2_label].map(lambda i: int(i.split("_")[1]))
        r = r.sort_values(by=[metric_name, 'index_p1', 'index_p2'], ascending=False)\
            .drop(['index_p1', 'index_p2'], axis=1).reset_index(drop=True)

        return r

    @staticmethod
    def number_of_communities(m):
        logger.info(f'number of communities')

        e_column = list(m.keys())
        c_column = [len(m[e].communities) for e in list(m.keys())]

        return pd.DataFrame(data={'epsilon': e_column, 'no. of communities': c_column})

    @staticmethod
    def pquality(m):
        logger.info(f'pquality of communities')

        pq_list = []
        for e in list(m.keys()):
            pq_list.append((e, m[e].scores['pquality']))

        return pq_list