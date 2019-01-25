import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datasources import PipelineIO
from datasources.database.database import db
from datasources.database.model import User, Profile, UserCommunity, UserEvent, Partition, Graph, Community
from sqlalchemy import func, desc, and_


class AnalysisHelper:
    """
    Helper class that supports analysis results in Jupyter Notebooks.
    """

    @staticmethod
    def load_ds_results(pipeline_outputs, ds_results):
        """Returns the desired pipeline outputs for a dataset.

        :param pipeline_outputs: dictionary which specifies the desired results names.

        At the first level of the dictionary are the pipeline stage names and
        at the second level, are a list of the desired output names.

        example:
        {
            'pipeline_name': ['output1', ...],
            ...
        }
        :param ds_results: single dataset output format that is returned after a pipeline execution.
        :return: dictionary with the desired results for a single dataset.

        At the first level of the dictionary are the pipeline stage names and
        at the second level, are a the desired output names together with the actual results.

        example:
        {
            'pipeline_name1': {
                'output_name1': {},
                ...
            },
            ...
        }
        """
        res = {}
        for pipeline_name, output_name_list in pipeline_outputs.items():
            res[pipeline_name] = PipelineIO.load_input(output_name_list, None, ds_results[pipeline_name])

        return res

    @staticmethod
    def load_all_results(pipeline_outputs, results):
        """Returns the desired pipeline outputs for all datesets.

        :param pipeline_outputs: dictionary which specifies the desired results names.

        At the first level of the dictionary are the pipeline stage names and
        at the second level, are a list of the desired output names.

        example:
        {
            'pipeline_name1': ['output_name1', ...],
            ...
        }
        :param results: output format that is returned after a pipeline execution.
        :return: dictionary with the desired results from all datasets.

        At the first level of the dictionary are the dataset names,
        at the second level are the pipeline stage names and
        at the third level, are a the desired output names together with the actual results.

        example:
        {
            'dataset_name1': {
                'pipeline_name1': {
                    'output_name1': {},
                    ...
                },
                ...
            },
            ...
        }
        """
        res = {}
        for ds_name, ds in results.items():
            res[ds_name] = AnalysisHelper.load_ds_results(pipeline_outputs, ds)

        return res

    @staticmethod
    def get_single_summary(pipeline_name, output_name, results):
        """Returns a merged summary of metrics for a desired output result.

        :param pipeline_name: name of the pipeline to which the desired output belongs to.
        :param output_name: name of the output to which the desired output belongs to.
        :param results: output format that is returned after a pipeline execution.
        :return: pandas dataframe.
        """

        # get results of interest
        filtered_results = AnalysisHelper.load_all_results({pipeline_name: [output_name]}, results)

        # set column 'name'
        for ds_name, ds in filtered_results.items():
            ds[pipeline_name][output_name]['name'] = ds_name

        # merge results in a single dataframe
        merge_results = pd.concat([ds[pipeline_name][output_name] for ds_name, ds in filtered_results.items()],
                                  sort=True).set_index('name')

        return merge_results

    @staticmethod
    def get_multi_summary(pipeline_name, output_name, results):
        """Returns multiple merged summary of metrics for a desired output result.

        :param pipeline_name: name of the pipeline to which the desired output belongs to.
        :param output_name: name of the output to which the desired output belongs to.
        :param results: output format that is returned after a pipeline execution.
        :return: dictionary of pandas dataframes, with the dataset names as the keys.
        """

        # get results of interest
        filtered_results = AnalysisHelper.load_all_results({pipeline_name: [output_name]}, results)

        # flatten results
        results = {ds_name: ds[pipeline_name][output_name] for ds_name, ds in filtered_results.items()}

        return results

    @staticmethod
    def community_over_nonodes_ratio(results):
        partitions_summary = AnalysisHelper.get_multi_summary('community_detection', 'partition_summary', results)

        return pd.DataFrame([{'name': ds_name, 'community/no_nodes ratio':  len(ds.index) / ds.no_nodes.sum()}
                             for ds_name, ds in partitions_summary.items()]).set_index('name')

    @staticmethod
    def plot_compare_cumsum_deg_dist(results):
        """Returns and plots the cumulative sum of degree distribution for all the datasets.

        :param results: output format that is returned after a pipeline execution.
        :return: dictionary of the cumulative degree distribution for all the datasets.
        With the datasets names as the keys.
        """
        from scipy import stats

        # get results of interest
        filtered_results = AnalysisHelper.load_all_results({'community_detection': ['cumsum_deg_dist']}, results)

        fig, ax = plt.subplots(figsize=(15, 8))

        for ds_name, ds in filtered_results.items():
            df = ds['community_detection']['cumsum_deg_dist']
            sns.lineplot(x=df['cumsum_of_the_no_of_nodes'], y=stats.zscore(df.index), ax=ax, label=ds_name) \
                .set_title('Cumulative sum of degree distribution')

        ax.axhline(0, ls='--')
        plt.xlabel('Cumulative sum of degrees')
        plt.ylabel('Number of nodes (normalized with z-score)')
        plt.legend()
        plt.tight_layout()
        plt.show()

        return filtered_results

    @staticmethod
    def __get_shared_nodes(results, pipeline_name, output_name):
        nodes = AnalysisHelper.get_single_summary(pipeline_name, output_name, results).user_name \
            .reset_index().drop_duplicates().user_name

        # get shared nodes
        shared_nodes = nodes.value_counts().where(lambda x: x > 1).dropna().astype(int).to_frame()\
            .rename(columns={'user_name': 'no_participations'})

        return shared_nodes

    @staticmethod
    def compare_common_nodes(results):
        # load nodes
        all_shared_nodes = AnalysisHelper.__get_shared_nodes(results, 'network_creation', 'nodes')
        cd_nodes = AnalysisHelper.__get_shared_nodes(results, 'community_detection', 'nodes')

        # check shared nodes survived
        all_shared_nodes['is_present'] = all_shared_nodes.index.isin(cd_nodes.index)

        return all_shared_nodes

    @staticmethod
    def get_common_nodes(results):
        """Returns and plots the shared nodes among multiple contexts.

        It defaults on the nodes chosen with the community detection algorithm.

        :param results: output format that is returned after a pipeline execution.
        :return: pandas series with user_names as indexes and number of appearances as values.
        """
        # get shared nodes
        shared_nodes = AnalysisHelper.__get_shared_nodes(results, 'community_detection', 'nodes')

        # add user information
        with db.session_scope() as session:
            userinfo = pd.read_sql(session.query(User, Profile.follower_rank).join(Profile.user)
                                   .filter(User.user_name.in_(shared_nodes.index.tolist())).statement,
                                   con=session.bind, index_col='user_name')

        shared_nodes = userinfo.merge(shared_nodes, left_index=True, right_index=True)\
            .drop(['id', 'following', 'followers', 'tweets', 'join_date'], axis=1)\
            .sort_values(by='no_participations', ascending=False)

        return shared_nodes

    @staticmethod
    def plot_events_with_common_nodes(results, pipeline_name='community_detection', output_name='nodes'):
        """Returns and plots the shared nodes among multiple contexts.

        It defaults on the nodes chosen with the community detection algorithm.

        :param results: output format that is returned after a pipeline execution.
        :param pipeline_name: name of the pipeline to which the desired output belongs to.
        :param output_name: name of the output to which the desired output belongs to.
        :return: pandas series with user_names as indexes and number of appearances as values.
        """
        from matplotlib.ticker import MaxNLocator

        # get results of interest
        filtered_results = AnalysisHelper.get_single_summary(pipeline_name, output_name, results)\
            .reset_index()[['user_name', 'name']]

        # get dummies from event names
        name_dummies = pd.get_dummies(filtered_results['name'])
        results = pd.concat([filtered_results['user_name'], name_dummies], axis=1)

        # sum all events appearances by user
        results = results.groupby('user_name').sum()

        # keep events with > 1 appearance
        results = results[results.sum(axis=1) > 1]

        # get total number of different users per event
        results = results.sum().to_frame().reset_index()

        results = results.rename(columns={'index': 'event_name', 0: 'total'}).sort_values(by='total', ascending=False)

        fig, ax = plt.subplots(figsize=(15, 8))
        sns.barplot(x='event_name', y='total', data=results)\
            .set_title('Number of users per event that appear in multiple events')
        for i, v in enumerate(results['total']):
            ax.text(i, v + .2, str(v), ha='center')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha='right')
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        plt.xlabel('Events')
        plt.ylabel('Number of users that appear in multiple events')
        plt.tight_layout()
        plt.show()
        fig.savefig("tables/foo.pdf", bbox_inches='tight')

        return results.set_index('event_name')

    @staticmethod
    def communities_summary_stats(results):
        # load results
        partitions_summary = AnalysisHelper.get_multi_summary('community_detection', 'partition_summary', results)
        no_all_nodes = AnalysisHelper.get_single_summary('network_creation', 'graph_summary', results)[['no_nodes']]\
            .rename(columns={'no_nodes': 'no_all_nodes'})
        no_cd_nodes = AnalysisHelper.get_single_summary('community_detection', 'nodes', results)[['user_name']]\
            .reset_index().drop_duplicates().set_index('name').groupby('name').count()\
            .rename(columns={'user_name': 'no_cd_nodes'})

        # compute stats for each context
        events_stats = no_all_nodes.merge(no_cd_nodes, left_index=True, right_index=True)

        communities = pd.DataFrame([{
            'name': ds_name, 'no_communities': len(ds.index), 'no_nodes_greatest_community': ds.no_nodes.max()
        } for ds_name, ds in partitions_summary.items()]).set_index('name')

        events_stats = events_stats.merge(communities, left_index=True, right_index=True)

        events_stats['is_degenerate'] = \
            events_stats.apply(lambda x: x['no_communities'] == 1 and
                                         x['no_nodes_greatest_community'] == x['no_all_nodes'], axis=1)

        # summarize each context
        good_contexts = events_stats[~events_stats['is_degenerate']]

        c_summary_dict = {
            'degenerated_context_ratio': events_stats['is_degenerate'].sum() / len(events_stats.index),
            'good_context_ratio': (~events_stats['is_degenerate']).sum() / len(events_stats.index),
            'avg_communities_per_good_context': good_contexts['no_communities'].sum() / len(good_contexts.index),
            'avg_sociable_users_ratio': (good_contexts['no_cd_nodes'] / good_contexts['no_all_nodes']).sum() / len(
                good_contexts.index)
        }
        c_summary = pd.DataFrame(data=list(c_summary_dict.values()), index=list(c_summary_dict.keys()))\
            .rename(columns={0: 'values'}).round(2)

        return c_summary

    @staticmethod
    def partitions_summary_aggregated(results):
        partitions_summary = AnalysisHelper.get_single_summary('community_detection', 'partition_summary', results)
        partitions_summary = partitions_summary.groupby(partitions_summary.index).describe()

        ps_list = []
        for name, ps in partitions_summary.groupby(level=0, axis=1):
            ps_list.append({
                'name': name,
                'count': ps[name]['count'].replace([np.inf, -np.inf], np.nan).dropna().mean(),
                'mean': ps[name]['mean'].replace([np.inf, -np.inf], np.nan).dropna().mean(),
                'std': ps[name]['std'].replace([np.inf, -np.inf], np.nan).dropna().mean(),
                'min': ps[name]['min'].replace([np.inf, -np.inf], np.nan).dropna().min(),
                'max': ps[name]['max'].replace([np.inf, -np.inf], np.nan).dropna().max()
            })

        return pd.DataFrame(ps_list).set_index('name').round(decimals=2)

    @staticmethod
    def pquality_aggregated(results):
        pipeline_name = 'community_detection'
        output_name = 'pquality'

        # get results of interest
        filtered_results = AnalysisHelper.load_all_results({pipeline_name: [output_name]}, results)
        for ds_name, ds in filtered_results.items():
            ds[pipeline_name][output_name]['name'] = ds_name
        merge_results = pd.concat([ds[pipeline_name][output_name] for ds_name, ds in filtered_results.items()],
                                  sort=True)

        partitions_summary = merge_results.groupby(merge_results.index).describe()

        partitions_summary = partitions_summary[[
            ('min', 'min'), ('max', 'max'), ('avg', 'mean'), ('std', 'mean')
        ]].round(decimals=2)
        partitions_summary.columns = partitions_summary.columns.droplevel(1)

        return partitions_summary

    @staticmethod
    def get_active_users():
        with db.session_scope() as session:
            active_users = pd.read_sql(session.query(User.id, User.user_name, User.tweets)
                                       .join(Profile)
                                       .filter(Profile.follower_rank > 0).statement,
                                       con=session.bind, index_col='id')

            active_users['tweets'] = (active_users['tweets'] - active_users['tweets'].min()) /\
                                     (active_users['tweets'].max() - active_users['tweets'].min())
            active_users = active_users[active_users.tweets > 0.00005]

        return active_users.index.tolist()

    @staticmethod
    def rank_1():
        active_users = AnalysisHelper.get_active_users()

        with db.session_scope() as session:
            userinfo = pd.read_sql(session.query(User.user_name, User.name, User.location,
                                                 (func.ifnull(func.sum(1 / UserCommunity.indegree_centrality), 1) +
                                                  func.ifnull(func.sum(UserEvent.topical_focus), 0)).label('rank'))
                                   .join(UserCommunity).join(UserEvent)
                                   .filter(User.id.in_(active_users))
                                   .group_by(UserCommunity.user_id)
                                   .order_by(desc('rank')).statement,
                                   con=session.bind).round(decimals=3)

        return userinfo

    @staticmethod
    def rank_2():
        active_users = AnalysisHelper.get_active_users()

        def min_max(df):
            min = df.min()
            max = df.max()

            return (df - min) / (max - min)

        with db.session_scope() as session:
            data = pd.read_sql(session.query(User.user_name,
                                             Profile.follower_rank,
                                             UserEvent.topical_attachment,
                                             UserCommunity.indegree_centrality)
                               .join(Profile, User.id == Profile.user_id)
                               .join(UserCommunity, Profile.user_id == UserCommunity.user_id)
                               .join(Community, UserCommunity.community_id == Community.id)
                               .join(Partition, Community.partition_id == Partition.id)
                               .join(Graph, Partition.graph_id == Graph.id)
                               .join(UserEvent, and_(Graph.event_id == UserEvent.event_id,
                                     User.id == UserEvent.user_id))
                               .filter(User.id.in_(active_users)).statement,
                               con=session.bind, index_col='user_name')

        data['topical_attachment'] = min_max(data['topical_attachment'])

        rank = data.groupby('user_name')\
            .apply(lambda x: abs(x['follower_rank'].head(1) - 1) *
                             (x['topical_attachment'].sum() + x['indegree_centrality'].sum()))\
            .reset_index(level=0, drop=True).sort_values(ascending=False)\
            .to_frame().rename(columns={'follower_rank': 'rank'})

        return rank
