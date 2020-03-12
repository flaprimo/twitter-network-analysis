import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datasources.database import User, Profile, Context, Graph


class AnalysisHelper:
    def __init__(self, datasources):
        self.datasources = datasources

    def __get_contexts_multiple(self, *file_args):
        df_list = [(context_name, self.datasources.files.read(*file_args, file_prefix=context_name))
                   for context_name in self.datasources.contexts.get_context_names()]

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

        return pd.DataFrame([{'name': context_name, 'community/no_nodes ratio': len(p.index) / p.no_nodes.sum()}
                             for context_name, p in partitions]).set_index('name').round(decimals=2)

    def get_pquality(self):
        return self.__get_contexts_multiple('community_detection_metrics', 'pquality', 'pquality', 'csv')

    def plot_cumsum_deg_dist(self):
        from scipy.stats import zscore
        cumsum_deg_dist = self.__get_contexts_multiple('network_metrics', 'cumsum_deg_dist', 'cumsum_deg_dist', 'csv')

        fig, ax = plt.subplots(figsize=(15, 8))
        for context_name, cdd in cumsum_deg_dist:
            sns.lineplot(x=cdd['cumsum_of_the_no_of_nodes'], y=zscore(cdd.index), ax=ax, label=context_name) \
                .set_title('Cumulative sum of degree distribution')

        ax.axhline(0, ls='--')
        plt.xlabel('Cumulative sum of degrees')
        plt.ylabel('Number of nodes (normalized with z-score)')
        plt.legend()
        plt.tight_layout()
        plt.savefig('foo.pdf', bbox_inches='tight')
        plt.show()

        return cumsum_deg_dist

    def partition_stats(self):
        partitions_summary = self.__get_contexts_multiple(
            'community_detection_metrics', 'partition_summary', 'partition_summary', 'csv')
        no_all_nodes = self.__get_contexts_single(
            'network_metrics', 'graph_summary', 'graph_summary', 'csv')[['no_nodes']] \
            .rename(columns={'no_nodes': 'no_all_nodes'})
        no_cd_nodes = self.__get_contexts_single('network_creation', 'create_nodes', 'nodes', 'csv')[['user_name']] \
            .reset_index().drop_duplicates().set_index('name').groupby('name').count() \
            .rename(columns={'user_name': 'no_cd_nodes'})

        # compute stats for each context
        context_stats = no_all_nodes.merge(no_cd_nodes, left_index=True, right_index=True)

        communities = pd.DataFrame([{
            'name': context_name, 'no_communities': len(p.index), 'no_nodes_greatest_community': p.no_nodes.max()
        } for context_name, p in partitions_summary]).set_index('name')

        context_stats = context_stats.merge(communities, left_index=True, right_index=True)

        context_stats['is_degenerate'] = \
            context_stats.apply(lambda x: x['no_communities'] == 1 and
                                          x['no_nodes_greatest_community'] == x['no_all_nodes'], axis=1)

        # summarize each context
        good_contexts = context_stats[~context_stats['is_degenerate']]

        c_summary_dict = {
            'degenerated_context_ratio': context_stats['is_degenerate'].sum() / len(context_stats.index),
            'good_context_ratio': (~context_stats['is_degenerate']).sum() / len(context_stats.index),
            'avg_communities_per_good_context': good_contexts['no_communities'].sum() / len(good_contexts.index),
            'avg_sociable_users_ratio': (good_contexts['no_cd_nodes'] / good_contexts['no_all_nodes']).sum() / len(
                good_contexts.index)
        }
        c_summary = pd.DataFrame(data=list(c_summary_dict.values()), index=list(c_summary_dict.keys())) \
            .rename(columns={0: 'values'}).round(2)

        return c_summary

    def __get_shared_nodes(self, *file_args):
        nodes = self.__get_contexts_single(*file_args).user_name.reset_index().drop_duplicates().user_name

        # get shared nodes
        shared_nodes = nodes.value_counts().where(lambda x: x > 1).dropna().astype(int).to_frame() \
            .rename(columns={'user_name': 'no_participations'})

        return shared_nodes

    def compare_common_nodes(self):
        # load nodes
        all_shared_nodes = self.__get_shared_nodes('network_creation', 'create_nodes', 'nodes', 'csv')
        cd_nodes = self.__get_shared_nodes('community_detection', 'add_communities_to_nodes', 'nodes', 'csv')

        # check shared nodes survived
        all_shared_nodes['is_present'] = all_shared_nodes.index.isin(cd_nodes.index)

        return all_shared_nodes

    def get_common_nodes(self):
        # get shared nodes
        shared_nodes = self.__get_shared_nodes(
            'community_detection', 'add_communities_to_nodes', 'nodes', 'csv')

        # add user information
        with self.datasources.database.session_scope() as session:
            userinfo = pd.read_sql(session.query(User, Profile.follower_rank).join(Profile.user)
                                   .filter(User.user_name.in_(shared_nodes.index.tolist())).statement,
                                   con=session.bind, index_col='user_name')

        shared_nodes = userinfo.merge(shared_nodes, left_index=True, right_index=True) \
            .drop(['id', 'following', 'followers', 'tweets', 'join_date'], axis=1) \
            .sort_values(by='no_participations', ascending=False)

        return shared_nodes

    def plot_contexts_with_common_nodes(self):
        from matplotlib.ticker import MaxNLocator

        nodes = self.__get_contexts_single(
            'community_detection', 'add_communities_to_nodes', 'nodes', 'csv').reset_index()[['user_name', 'name']]

        # get dummies from event names
        name_dummies = pd.get_dummies(nodes['name'])
        node_participations = pd.concat([nodes['user_name'], name_dummies], axis=1)

        # sum all events appearances by user
        node_participations = node_participations.groupby('user_name').sum()

        # keep events with > 1 appearance
        node_participations = node_participations[node_participations.sum(axis=1) > 1]

        # get total number of different users per context
        node_participations = node_participations.sum().to_frame().reset_index()

        node_participations = node_participations \
            .rename(columns={'index': 'context_name', 0: 'total'}).sort_values(by='total', ascending=False)

        fig, ax = plt.subplots(figsize=(15, 8))
        sns.barplot(x='context_name', y='total', data=node_participations) \
            .set_title('Number of users per event that appear in multiple contexts')
        for i, v in enumerate(node_participations['total']):
            ax.text(i, v + .2, str(v), ha='center')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha='right')
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        plt.xlabel('Events')
        plt.ylabel('Number of users that appear in multiple contexts')
        plt.tight_layout()
        plt.show()

        return node_participations.set_index('context_name')

    def get_all_nodes(self):
        return self.__get_contexts_single('community_detection', 'add_communities_to_nodes', 'nodes', 'csv')

    def get_rank_1(self):
        return self.datasources.files.read('ranking', 'rank_1', 'rank_1', 'csv')

    def get_rank_2(self):
        return self.datasources.files.read('ranking', 'rank_2', 'rank_2', 'csv')

    def get_rank_3(self):
        return self.datasources.files.read('ranking', 'rank_3', 'rank_3', 'csv')

    def get_hashtags_corr(self):
        return self.datasources.files.read('hashtags_vector', 'get_corr_hashtags', 'corr_hashtags', 'csv')

    def get_users_corr(self):
        return self.datasources.files.read('hashtags_vector', 'get_corr_users', 'corr_users', 'csv')

    def get_bow(self):
        return self.datasources.files.read('hashtags_vector', 'get_users_hashtags_bow', 'users_hashtags_bow', 'csv')

    def get_hashtags_network(self):
        return self.datasources.files.read('bipartite_graph', 'get_hashtag_network', 'hashtag_network', 'csv')

    def get_users_network(self):
        return self.datasources.files.read('bipartite_graph', 'get_user_network', 'user_network', 'csv')

    def get_user_timelines(self):
        return self.datasources.files.read('user_timelines', 'get_user_timelines', 'user_timelines', 'csv')

    def show_peaks(self, hashtag_list):
        hashtag_frequency = \
            self.datasources.files.read('context_detector', 'hashtags_frequency', 'hashtags_frequency', 'csv')
        hashtag_peaks = \
            self.datasources.files.read('context_detector', 'find_peaks', 'hashtags_peaks', 'csv')

        hashtag_frequency = hashtag_frequency[
            hashtag_frequency['hashtag'].isin(hashtag_peaks['hashtag'].drop_duplicates())]

        if hashtag_list:
            hashtag_frequency = hashtag_frequency[hashtag_frequency['hashtag'].isin(hashtag_list)]
            hashtag_peaks = hashtag_peaks[hashtag_peaks['hashtag'].isin(hashtag_list)]

        for hashtag, timeline in hashtag_frequency.groupby('hashtag'):
            empty_timeline = pd.date_range(timeline['date'].min(), timeline['date'].max())
            timeline = timeline.set_index('date')['count']
            timeline = timeline.reindex(empty_timeline, fill_value=0).to_frame()

            peaks = hashtag_peaks[hashtag_peaks['hashtag'] == hashtag]
            ranges = peaks.apply(lambda x: pd.date_range(x['start_date'], x['end_date']).date, axis=1).explode()

            timeline['is_peak_interval'] = timeline.index.isin(ranges)
            timeline['is_peak'] = timeline.index.isin(peaks['peak_date'])

            # plotting
            fig, ax = plt.subplots(figsize=(20, 6))
            ax.step(timeline.index, timeline['count'], color='blue')
            ax.fill_between(timeline.index, (timeline['count'].max() * timeline['is_peak_interval']),
                            step='pre', alpha=0.4, color='red')
            ax.plot(timeline.index, (timeline['count'].max() * timeline['is_peak_interval']),
                    color='red', drawstyle='steps')
            ax.scatter(x=timeline.index, y=(timeline['count'] * timeline['is_peak']).replace(0, np.nan),
                       color='red', marker='o', s=100)
            ax.axhline(y=0, color='gray', linestyle='--', lw=1)

            plt.title(hashtag)
            ax.set_xlabel('dates (d)')
            ax.set_ylabel('hashtag frequency')
            plt.xticks(ticks=timeline.index.values, rotation=90)
            plt.savefig(f'peak-{hashtag}.pdf', bbox_inches='tight')
            plt.show()

    # TABLES
    # table 1 - context summaries
    def get_table1(self):
        table_1 = self.get_contexts()[['start_date', 'end_date', 'hashtags']] \
            .merge(self.get_graphs(), left_index=True, right_index=True)
        table_1.reset_index(inplace=True)

        table_1['hashtags'] = table_1['hashtags'].apply(
            lambda x: x[0].lower() if len(x) > 1 else x[0].lower() + ', ...')
        table_1['name'] = table_1['name'].str.replace('-', ' ').str.capitalize()

        # time period
        is_same_year = table_1['start_date'].min() == table_1['start_date'].max()
        time_period = table_1['start_date'].min().strftime('%Y') + \
                      ('' if is_same_year else f'/{table_1["start_date"].max().strftime("%y")}')
        time_period = f'period ({time_period})'

        table_1[time_period] = table_1[['start_date', 'end_date']] \
            .apply(lambda x: x['start_date'].strftime('%m-%d') + ' / ' + x['end_date'].strftime('%m-%d'), axis=1) \
            if is_same_year else \
            table_1[['start_date', 'end_date']] \
                .apply(lambda x: x['start_date'].strftime('%y-%m-%d') + ' / ' + x['end_date'].strftime('%y-%m-%d'),
                       axis=1)

        # graph stats
        table_1[['assortativity', 'avg_degree']] = table_1[['assortativity', 'avg_degree']].round(decimals=1)
        table_1['density'] = table_1['density'].round(decimals=3)

        table_1 = table_1[['name', time_period, 'no_nodes', 'no_edges', 'density', 'avg_degree', 'assortativity']]

        table_1.rename(columns={'name': 'context name',
                                'avg_degree': 'avg degree',
                                'no_nodes': 'nodes',
                                'no_edges': 'edges'}, inplace=True)
        table_1.columns = [c.capitalize() for c in table_1.columns]

        return table_1

    # table 3 - top repeated users
    def get_table3(self):
        table_3 = self.get_common_nodes().reset_index()
        table_3 = table_3.head(11).round(decimals=2).sort_values(by=['no_participations', 'follower_rank'],
                                                                 ascending=False)
        table_3.rename(columns={'index': 'username'}, inplace=True)
        table_3.drop(columns=['url', 'bio', 'location'], inplace=True)
        table_3.rename(columns={'follower_rank': 'follower rank',
                                'no_participations': 'participations'}, inplace=True)
        table_3.columns = [c.capitalize() for c in table_3.columns]

        return table_3

    # table 6 - ranks comparison
    def get_table6(self):
        ranks = {f'Rank {i}': self.datasources.files.read('ranking', f'rank_{i}', f'rank_{i}', 'csv')['user_name']
            .head(100).reset_index(drop=True) for i in [1, 2, 3]}
        ranks = pd.DataFrame(ranks)
        ranks.index += 1
        ranks.index.name = '#'

        return ranks

    def plot_tweets_distribution(self):
        cd_config = self.datasources.context_detection.get_config()
        rank = self.datasources.files.read('ranking', cd_config['rank'], cd_config['rank'], 'csv')['user_name']
        cd_config = self.datasources.context_detection.get_config()
        user_timelines = self.datasources.files.read(
            'user_timelines', 'get_user_timelines', 'user_timelines', 'csv')['user_name']

        print(f'Users selected for harvesting: '
              f'{cd_config["top_no_users"]} (actual {user_timelines.nunique()})/ {rank.nunique()}\n'
              f'Number of posted tweets: {len(user_timelines)}')

        user_timelines = user_timelines[user_timelines.isin(rank.head(cd_config['top_no_users']))]
        user_timelines = user_timelines.groupby(user_timelines).size().sort_values()

        print(f'Users posted a min of {user_timelines.min()} and a max of {user_timelines.max()} tweets')

        user_timelines_byuser_cumsum = user_timelines.cumsum().reset_index(drop=True)

        fig, ax = plt.subplots(figsize=(15, 8))
        ax.plot(user_timelines_byuser_cumsum)
        ax.fill_between(user_timelines_byuser_cumsum.index.values, user_timelines_byuser_cumsum, alpha=0.4)

        plt.xlabel('Number of users')
        plt.ylabel('Number of tweets')
        plt.title('Cumulative number of published tweets per user')
        plt.ylim(ymin=0, ymax=user_timelines_byuser_cumsum.max())
        plt.xlim(xmin=0, xmax=len(user_timelines_byuser_cumsum)-1)
        plt.locator_params(axis='x', nbins=10)
        plt.tight_layout()
        plt.savefig('tweets-distribution.pdf', bbox_inches='tight')
        plt.show()

    @staticmethod
    def print_full(x):
        pd.set_option('display.max_rows', len(x))
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 2000)
        pd.set_option('display.float_format', '{:20,.2f}'.format)
        pd.set_option('display.max_colwidth', -1)
        print(x)
        pd.reset_option('display.max_rows')
        pd.reset_option('display.max_columns')
        pd.reset_option('display.width')
        pd.reset_option('display.float_format')
        pd.reset_option('display.max_colwidth')
