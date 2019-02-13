import pandas as pd
import numpy as np
from datasources.database import User, Profile, Context, Graph, Partition, Community, UserCommunity, UserContext


class AnalysisHelper:
    def __init__(self, datasources):
        self.datasources = datasources

    def __get_contexts_multiple(self, *file_args):
        df_list = []
        for context_name in self.datasources.contexts.get_context_names():
            df = self.datasources.files.read(*file_args, file_prefix=context_name)
            df_list.append((context_name, df))

        return df_list

    def __get_contexts_single(self, *file_args):
        df_list = self.__get_contexts_multiple(*file_args)

        for context_name, partition in df_list:
            partition['name'] = context_name

        merged_df_list = pd.concat([p for _, p in df_list], sort=True).set_index('name')

        return merged_df_list

    def get_contexts(self):
        with self.datasources.database.session_scope() as session:
            contexts = pd.read_sql(session.query(Context).statement,
                                   con=session.bind, index_col='name')
        contexts.drop(columns='id', inplace=True)

        return contexts

    def get_graphs(self):
        with self.datasources.database.session_scope() as session:
            graphs = pd.read_sql(session.query(Context.name, Graph).join(Graph).statement,
                                 con=session.bind, index_col='name')
        graphs.drop(columns=['id', 'context_id'], inplace=True)
        graphs['scc_over_nodes'] = graphs.apply(lambda x: x['strongly_conn_components'] / x['no_nodes'], axis=1)
        graphs = graphs.round(decimals=3)

        return graphs

    def get_partitions(self):
        return self.__get_contexts_multiple(
            'community_detection_metrics', 'partition_summary', 'partition_summary', 'csv')

    def get_partitions_aggregated(self):
        merged_partitions = self.__get_contexts_single(
            'community_detection_metrics', 'partition_summary', 'partition_summary', 'csv')
        merged_partitions = merged_partitions.groupby(merged_partitions.index).describe()

        p_list = []
        for name, p in merged_partitions.groupby(level=0, axis=1):
            p_list.append({
                'name': name,
                'count': p[name]['count'].replace([np.inf, -np.inf], np.nan).dropna().mean(),
                'mean': p[name]['mean'].replace([np.inf, -np.inf], np.nan).dropna().mean(),
                'std': p[name]['std'].replace([np.inf, -np.inf], np.nan).dropna().mean(),
                'min': p[name]['min'].replace([np.inf, -np.inf], np.nan).dropna().min(),
                'max': p[name]['max'].replace([np.inf, -np.inf], np.nan).dropna().max()
            })

        return pd.DataFrame(p_list).set_index('name').round(decimals=2)

    def community_over_nodes_ratio(self):
        partitions = self.__get_contexts_multiple(
            'community_detection_metrics', 'partition_summary', 'partition_summary', 'csv')

        return pd.DataFrame([{'name': context_name, 'community/no_nodes ratio':  len(p.index) / p.no_nodes.sum()}
                             for context_name, p in partitions]).set_index('name').round(decimals=2)

    def get_pquality(self):
        return self.__get_contexts_multiple('community_detection_metrics', 'pquality', 'pquality', 'csv')
