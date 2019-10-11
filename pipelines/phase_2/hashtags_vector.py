import logging
import pandas as pd
from pipelines.pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class HashtagsVector(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_users_hashtags_bow',
                'file_name': 'users_hashtags_bow',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_name': str
                    },
                    'index_col': 'user_name'
                }
            },
            {
                'stage_name': 'get_corr_hashtags',
                'file_name': 'corr_hashtags',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'hashtag': str
                    },
                    'index_col': 'hashtag'
                }
            },
            {
                'stage_name': 'get_corr_users',
                'file_name': 'corr_users',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_name': str
                    },
                    'index_col': 'user_name'
                }
            },
            {
                'stage_name': 'cluster_hashtags',
                'file_name': 'clusters',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_name': str,
                        'cluster': 'uint16',
                    },
                    'index_col': 'user_name'
                }
            },
            {
                'stage_name': 'biggest_cluster_users',
                'file_name': 'users',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'user_name': str,
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__get_users_hashtags_bow, self.__get_corr_users]  # , self.__cluster_hashtags, self.__biggest_cluster_users]
        super(HashtagsVector, self).__init__('hashtags_vector', files, tasks, datasources)

    def __get_users_hashtags_bow(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_users_hashtags_bow', 'users_hashtags_bow', 'csv'):
            hashtags_users_network = self.datasources.files.read(
                'bipartite_graph', 'get_hashtags_users_network', 'hashtags_users_network', 'csv')

            # pivot hashtags and counts wrt user_name
            hashtags_users_bow = \
                hashtags_users_network.pivot_table(index='user_name', columns='hashtag', values='weight', fill_value=0)

            self.datasources.files.write(
                hashtags_users_bow, 'hashtags_vector', 'get_users_hashtags_bow', 'users_hashtags_bow', 'csv')

    def __get_corr_hashtags(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_corr_hashtags', 'corr_hashtags', 'csv'):
            hashtags_users_bow = self.datasources.files.read(
                'hashtags_vector', 'get_users_hashtags_bow', 'users_hashtags_bow', 'csv')

            # set as sparse
            sparse_dtype = pd.SparseDtype('Int64', fill_value=0)
            hashtags_users_bow.astype(sparse_dtype)

            corr_hashtags = hashtags_users_bow.corr()

            self.datasources.files.write(
                corr_hashtags, 'hashtags_vector', 'get_corr_hashtags', 'corr_hashtags', 'csv')

    def __get_corr_users(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_corr_users', 'corr_users', 'csv'):
            hashtags_users_bow = self.datasources.files.read(
                'hashtags_vector', 'get_users_hashtags_bow', 'users_hashtags_bow', 'csv')

            # set as sparse
            sparse_dtype = pd.SparseDtype('Int64', fill_value=0)
            hashtags_users_bow.astype(sparse_dtype)

            corr_hashtags = hashtags_users_bow.T.corr()

            self.datasources.files.write(
                corr_hashtags, 'hashtags_vector', 'get_corr_users', 'corr_users', 'csv')

    def __cluster_hashtags(self):
        if not self.datasources.files.exists('hashtags_vector', 'cluster_hashtags', 'clusters', 'csv'):
            hashtags_users_bow = self.datasources.files.read(
                'hashtags_vector', 'get_users_hashtags_bow', 'users_hashtags_bow', 'csv')

            from sklearn.cluster import KMeans
            from sklearn.decomposition import TruncatedSVD
            from sklearn.preprocessing import Normalizer
            from sklearn.pipeline import make_pipeline
            import pandas as pd
            from sklearn import metrics

            svd = TruncatedSVD(n_components=150, n_iter=7, random_state=42)
            svd.fit(hashtags_users_bow)
            print(svd.explained_variance_ratio_)
            print(svd.explained_variance_ratio_.sum())
            print(svd.singular_values_)

            explained_variance = svd.explained_variance_ratio_.sum()
            print("Explained variance of the SVD step: {}%".format(int(explained_variance * 100)))

            normalizer = Normalizer(copy=False)
            lsa = make_pipeline(svd, normalizer)

            X = lsa.fit_transform(hashtags_users_bow)

            km = KMeans(n_clusters=2)
            km.fit(X)
            # Get cluster assignment labels
            labels = km.labels_
            # Format results as a DataFrame
            results = pd.DataFrame([hashtags_users_bow.index, labels]).T.rename(columns={0: 'user_name', 1: 'cluster'})

            print("Homogeneity: %0.3f" % metrics.homogeneity_score(labels, km.labels_))
            print("Completeness: %0.3f" % metrics.completeness_score(labels, km.labels_))
            print("V-measure: %0.3f" % metrics.v_measure_score(labels, km.labels_))
            print("Adjusted Rand-Index: %.3f"
                  % metrics.adjusted_rand_score(labels, km.labels_))
            print("Silhouette Coefficient: %0.3f"
                  % metrics.silhouette_score(X, km.labels_, sample_size=1000))

            self.datasources.files.write(results, 'hashtags_vector', 'cluster_hashtags', 'clusters', 'csv')

    def __biggest_cluster_users(self):
        if not self.datasources.files.exists('hashtags_vector', 'biggest_cluster_users', 'users', 'csv'):
            hashtags_users_bow = self.datasources.files.read(
                'hashtags_vector', 'get_users_hashtags_bow', 'users_hashtags_bow', 'csv')

            cluster_sizes = hashtags_users_bow.groupby('cluster').size().reset_index(name='counts')
            biggest_cluster = int(cluster_sizes.iloc[cluster_sizes['counts'].idxmin()]['cluster'])
            user_name_list = pd.DataFrame(
                hashtags_users_bow[hashtags_users_bow['cluster'] == biggest_cluster].index, columns=['user_name'])

            self.datasources.files.write(user_name_list, 'hashtags_vector', 'biggest_cluster_users', 'users', 'csv')
