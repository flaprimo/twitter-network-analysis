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
        merge_results = pd.concat([ds[pipeline_name][output_name] for ds_name, ds in filtered_results.items()])\
            .set_index('name')

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
