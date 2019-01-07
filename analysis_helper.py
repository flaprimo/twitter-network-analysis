import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datasources import PipelineIO
from datasources.database.database import db
from datasources.database.model import User, Profile


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
        :param ds_result
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

        return pd.DataFrame([{'name': ds_name, 'community/no_nodes ratio': ds.no_nodes.sum() / len(ds.index)}
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
        :param pipeline_name: name of the pipeline to which the desired output belongs to.
        :param output_name: name of the output to which the desired output belongs to.
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

        return results.set_index('event_name')
