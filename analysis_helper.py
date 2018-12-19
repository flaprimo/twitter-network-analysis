import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datasources import PipelineIO


class AnalysisHelper:
    # INPUT:
    # {
    #     'pipeline_name': ['output1', 'output2'],
    #     'pipeline_name': ['output1', 'output2']
    # }
    #
    # OUTPUT:
    # {
    #     'pipeline_name': {
    #         'output1': {},
    #         'output2': {}
    #     }
    # }
    @staticmethod
    def load_ds_results(pipeline_outputs, ds_results):
        res = {}
        for pipeline_name, output_name_list in pipeline_outputs.items():
            res[pipeline_name] = PipelineIO.load_input(output_name_list, None, ds_results[pipeline_name])

        return res

    @staticmethod
    def load_all_results(pipeline_outputs, results):
        res = {}
        for ds_name, ds in results.items():
            res[ds_name] = AnalysisHelper.load_ds_results(pipeline_outputs, ds)

        return res

    @staticmethod
    def get_single_summary(pipeline_name, output_name, results):
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
        # get results of interest
        filtered_results = AnalysisHelper.load_all_results({pipeline_name: [output_name]}, results)

        # flatten results
        results = {ds_name: ds[pipeline_name][output_name] for ds_name, ds in filtered_results.items()}

        return results

    @staticmethod
    def plot_compare_cumsum_deg_dist(results):
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
    def get_common_nodes(pipeline_name, output_name, results):  # 'community_detection', 'nodes'
        # get results of interest
        filtered_results = AnalysisHelper.get_single_summary(pipeline_name, output_name, results)

        results = filtered_results.reset_index()['user_name'].value_counts().to_frame()
        results = results[results['user_name'] > 1]

        return results

    @staticmethod
    def plot_events_with_common_nodes(pipeline_name, output_name, results):  # 'community_detection', 'nodes'
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

        results.rename(columns={'index': 'event_name', 0: 'total'}, inplace=True)

        fig, ax = plt.subplots(figsize=(15, 8))
        sns.barplot(x='event_name', y='total', data=results)\
            .set_title('Number of users per event that appear in more than one event')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        plt.xlabel('Events')
        plt.ylabel('Number of users that appear in more than one event')
        plt.tight_layout()
        plt.show()

        return results
