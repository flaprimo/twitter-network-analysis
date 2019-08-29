import logging
import pandas as pd
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class HashtagsVector(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_users_network',
                'file_name': 'users_network',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'from_username': str,
                        'to_username': str,
                        'weight': 'uint16'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_hashtags_network',
                'file_name': 'hashtags_network',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'from_hashtag': str,
                        'to_hashtag': str,
                        'weight': 'uint16'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'get_bag_of_words',
                'file_name': 'hashtags_bow',
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
        tasks = [[self.__get_users_network, self.__get_hashtags_network, self.__get_bag_of_words],
                 self.__get_corr_users]
        #  , self.__cluster_hashtags, self.__biggest_cluster_users]
        super(HashtagsVector, self).__init__('hashtags_vector', files, tasks, datasources)

    def __get_users_network(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_users_network', 'users_network', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')[['user_name', 'mentions']]

            # create user edges
            users_network = user_timelines[user_timelines['mentions'].map(lambda m: len(m) > 0)].explode('mentions') \
                .rename(columns={'user_name': 'from_username', 'mentions': 'to_username'})\
                .drop_duplicates()

            # count users co-occurrences
            users_network['from_username'], users_network['to_username'] = \
                users_network.min(axis=1), users_network.max(axis=1)
            users_network['weight'] = 1
            users_network = users_network.groupby(['from_username', 'to_username']).sum().reset_index()

            self.datasources.files.write(
                users_network, 'hashtags_vector', 'get_users_network', 'users_network', 'csv')

    def __get_hashtags_network(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_hashtags_network', 'hashtags_network', 'csv'):
            hashtags_network = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')['hashtags']

            # pair co-occurred hashtags
            hashtags_network = hashtags_network \
                .map(lambda h_list: [(h1, h2) if h1 < h2 else (h2, h1)
                                     for i, h1 in enumerate(h_list)
                                     for h2 in h_list[:i]])

            # create hashtag edges
            hashtags_network = hashtags_network.explode().dropna()
            hashtags_network = pd.DataFrame(hashtags_network.tolist(),
                                            columns=['from_hashtag', 'to_hashtag'],
                                            index=hashtags_network.index)

            # count hashtags co-occurrences
            hashtags_network['weight'] = 1
            hashtags_network = hashtags_network.groupby(['from_hashtag', 'to_hashtag']).sum().reset_index()

            self.datasources.files.write(
                hashtags_network, 'hashtags_vector', 'get_hashtags_network', 'hashtags_network', 'csv')

    def __get_bag_of_words(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'filter_user_timelines', 'filtered_user_timelines', 'csv')[['user_name', 'hashtags']]

            # hashtag list per user_name
            hashtags_vector = user_timelines.groupby('user_name').sum()

            # explode hashtags
            hashtags_vector = hashtags_vector.explode('hashtags')\
                .rename(columns={'hashtags': 'hashtag'}).reset_index().dropna()

            # count hashtags wrt user_name
            hashtags_vector = hashtags_vector.groupby(['user_name', 'hashtag']).size().reset_index(name='counts') \
                .sort_values(by=['user_name', 'counts', 'hashtag'], ascending=[True, False, True]) \
                .reset_index(drop=True)

            # pivot hashtags and counts wrt user_name
            hashtags_vector = \
                hashtags_vector.pivot_table(index='user_name', columns='hashtag', values='counts', fill_value=0)

            self.datasources.files.write(
                hashtags_vector, 'hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv')

    def __get_corr_hashtags(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_corr_hashtags', 'corr_hashtags', 'csv'):
            bag_of_words = self.datasources.files.read('hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv')

            # set as sparse
            sparse_dtype = pd.SparseDtype('Int64', fill_value=0)
            bag_of_words.astype(sparse_dtype)

            corr_hashtags = bag_of_words.corr()

            self.datasources.files.write(
                corr_hashtags, 'hashtags_vector', 'get_corr_hashtags', 'corr_hashtags', 'csv')

    def __get_corr_users(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_corr_users', 'corr_users', 'csv'):
            bag_of_words = self.datasources.files.read('hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv')

            # set as sparse
            sparse_dtype = pd.SparseDtype('Int64', fill_value=0)
            bag_of_words.astype(sparse_dtype)

            corr_hashtags = bag_of_words.T.corr()

            self.datasources.files.write(
                corr_hashtags, 'hashtags_vector', 'get_corr_users', 'corr_users', 'csv')

    def __cluster_hashtags(self):
        if not self.datasources.files.exists('hashtags_vector', 'cluster_hashtags', 'clusters', 'csv'):
            hashtags_vector = \
                self.datasources.files.read('hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv')

            from sklearn.cluster import KMeans
            from sklearn.decomposition import TruncatedSVD
            from sklearn.preprocessing import Normalizer
            from sklearn.pipeline import make_pipeline
            import pandas as pd
            from sklearn import metrics

            svd = TruncatedSVD(n_components=150, n_iter=7, random_state=42)
            svd.fit(hashtags_vector)
            print(svd.explained_variance_ratio_)
            print(svd.explained_variance_ratio_.sum())
            print(svd.singular_values_)

            explained_variance = svd.explained_variance_ratio_.sum()
            print("Explained variance of the SVD step: {}%".format(int(explained_variance * 100)))

            normalizer = Normalizer(copy=False)
            lsa = make_pipeline(svd, normalizer)

            X = lsa.fit_transform(hashtags_vector)

            km = KMeans(n_clusters=2)
            km.fit(X)
            # Get cluster assignment labels
            labels = km.labels_
            # Format results as a DataFrame
            results = pd.DataFrame([hashtags_vector.index, labels]).T.rename(columns={0: 'user_name', 1: 'cluster'})

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
            hashtags_vector = self.datasources.files.read('hashtags_vector', 'cluster_hashtags', 'clusters', 'csv')

            cluster_sizes = hashtags_vector.groupby('cluster').size().reset_index(name='counts')
            biggest_cluster = int(cluster_sizes.iloc[cluster_sizes['counts'].idxmin()]['cluster'])
            user_name_list = pd.DataFrame(
                hashtags_vector[hashtags_vector['cluster'] == biggest_cluster].index, columns=['user_name'])

            self.datasources.files.write(user_name_list, 'hashtags_vector', 'biggest_cluster_users', 'users', 'csv')
