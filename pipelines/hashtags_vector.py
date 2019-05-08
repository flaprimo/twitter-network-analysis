import logging
import pandas as pd
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class HashtagsVector(PipelineBase):
    def __init__(self, datasources):
        files = [
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
        tasks = [self.__get_bag_of_words]  #, self.__get_corr_users, self.__cluster_hashtags, self.__biggest_cluster_users]
        super(HashtagsVector, self).__init__('hashtags_vector', files, tasks, datasources)

    def __get_bag_of_words(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv'):
            user_timelines = self.datasources.files.read(
                'user_timelines', 'get_user_timelines', 'user_timelines', 'csv')

            # limit users and tweets
            n_users = 300
            n_tws = 200
            rank_2 = self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')['user_name'].head(n_users)
            user_timelines = user_timelines[user_timelines['user_name'].isin(rank_2)]
            user_timelines = user_timelines.groupby('user_name')\
                .apply(lambda x: x.sort_values(by='date', ascending=True).head(n_tws)).reset_index(drop=True)

            # hashtag list per user_name
            hashtags_vector = user_timelines[['user_name', 'hashtags']].groupby('user_name').sum()
            # hashtags_vector['hashtags'] =\
            #     hashtags_vector['hashtags'].apply(lambda h_list: [(x, h_list.count(x)) for x in set(h_list)])

            # explode and count hashtags wrt user_name
            hashtags_vector = hashtags_vector['hashtags'].apply(pd.Series) \
                .merge(hashtags_vector, right_index=True, left_index=True) \
                .drop(['hashtags'], axis=1).reset_index() \
                .melt(id_vars=['user_name'], value_name='hashtag').drop('variable', axis=1).dropna() \
                .groupby(['user_name', 'hashtag']).size().reset_index(name='counts') \
                .sort_values(by=['user_name', 'counts', 'hashtag'], ascending=[True, False, True])\
                .reset_index(drop=True)

            # pivot hashtags and counts wrt user_name
            hashtags_vector = \
                hashtags_vector.pivot_table(index='user_name', columns='hashtag', values='counts', fill_value=0)

            self.datasources.files.write(
                hashtags_vector, 'hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv')

    def __get_corr_hashtags(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_corr_hashtags', 'corr_hashtags', 'csv'):
            bag_of_words = self.datasources.files.read('hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv')

            corr_hashtags = bag_of_words.corr()

            self.datasources.files.write(
                corr_hashtags, 'hashtags_vector', 'get_corr_hashtags', 'corr_hashtags', 'csv')

    def __get_corr_users(self):
        if not self.datasources.files.exists('hashtags_vector', 'get_corr_users', 'corr_users', 'csv'):
            bag_of_words = self.datasources.files.read('hashtags_vector', 'get_bag_of_words', 'hashtags_bow', 'csv')\
                .to_sparse()

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
