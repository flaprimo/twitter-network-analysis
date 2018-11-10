import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


class AnalysisHelper:
    @staticmethod
    def filter_dataset_results(result_name, results):
        return [(ds_name, ds_results[0][result_name])
                for ds_name, ds_results in results.items()]

    @staticmethod
    def merge_dataset_results(result_name, results):
        # get results of interest
        filtered_results = AnalysisHelper.filter_dataset_results(result_name, results)
        # add dataset name as column
        for ds_name, df in filtered_results:
            df['dataset_name'] = ds_name
        # merge results in a single dataframe
        merge_results = pd.concat([df for name, df in filtered_results]).set_index('dataset_name')

        return merge_results

    @staticmethod
    def plot_compare_cumsum_deg_dist(results):
        from scipy import stats

        filtered_results = AnalysisHelper.filter_dataset_results('cumsum_deg_dist', results)

        fig, ax = plt.subplots(figsize=(15, 8))

        for ds_name, df in filtered_results:
            sns.lineplot(x=df['cumsum_of_the_no_of_nodes'], y=stats.zscore(df.index), ax=ax, label=ds_name) \
                .set_title('Cumulative sum of degree distribution')

        ax.axhline(0, ls='--')
        plt.xlabel('Cumulative sum of degrees')
        plt.ylabel('Number of nodes (normalized with z-score)')
        plt.legend()
        plt.tight_layout()
        plt.show()
