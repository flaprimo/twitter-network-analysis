import logging
import pandas as pd
from sqlalchemy import func, desc, and_
from datasources.database import User, Profile, Graph, Partition, Community, UserCommunity, UserContext
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class Ranking(PipelineBase):
    def __init__(self, datasources):
        files = [
            {
                'stage_name': 'get_active_users',
                'file_name': 'active_users',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'id': 'uint32',
                        'user_name': str
                    },
                    'index_col': 'id'
                }
            },
            {
                'stage_name': 'rank_1',
                'file_name': 'rank_1',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'id': 'uint32',
                        'user_name': str,
                        'rank': 'uint'
                    },
                    'index_col': 'id'
                }
            },
            {
                'stage_name': 'rank_2',
                'file_name': 'rank_2',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'id': 'uint32',
                        'user_name': str,
                        'rank': 'uint'
                    },
                    'index_col': 'id'
                }
            },
            {
                'stage_name': 'rank_3',
                'file_name': 'rank_3',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        'id': 'uint32',
                        'user_name': str,
                        'rank': 'uint'
                    },
                    'index_col': 'id'
                }
            }
        ]
        tasks = [self.__get_active_users, self.__rank_1, self.__rank_2, self.__rank_3]
        super(Ranking, self).__init__('ranking', files, tasks, datasources)

    @staticmethod
    def __min_max(df):
        min = df.min()
        max = df.max()

        return (df - min) / (max - min)

    def __get_active_users(self):
        with self.datasources.database.session_scope() as session:
            active_users = pd.read_sql(session.query(User.id, User.user_name, User.tweets)
                                       .join(Profile)
                                       .filter(Profile.follower_rank > 0).statement,
                                       con=session.bind, index_col='id')

            active_users['tweets'] = self.__min_max(active_users['tweets'])
            active_users = active_users[active_users.tweets > 0.00005]
            active_users.drop(columns='tweets', inplace=True)

        self.datasources.files.write(active_users, 'ranking', 'get_active_users', 'active_users', 'csv')

    def __rank_1(self):
        active_users = self.datasources.files.read('ranking', 'get_active_users', 'active_users', 'csv').index.tolist()

        with self.datasources.database.session_scope() as session:
            rank = pd.read_sql(session.query(User.id, User.user_name,
                                             (func.ifnull(func.sum(1 / UserCommunity.indegree_centrality), 1) +
                                              func.ifnull(func.sum(UserContext.topical_focus), 0)).label('rank'))
                               .join(UserCommunity).join(UserContext)
                               .filter(User.id.in_(active_users))
                               .group_by(UserCommunity.user_id)
                               .order_by(desc('rank'), User.user_name.asc()).statement,
                               con=session.bind).round(decimals=3)

        self.datasources.files.write(rank, 'ranking', 'rank_1', 'rank_1', 'csv')

    def __rank_2(self):
        active_users = self.datasources.files.read('ranking', 'get_active_users', 'active_users', 'csv').index.tolist()

        with self.datasources.database.session_scope() as session:
            data = pd.read_sql(session.query(User.id, User.user_name,
                                             Profile.follower_rank,
                                             UserContext.topical_attachment,
                                             UserCommunity.indegree_centrality)
                               .join(Profile, User.id == Profile.user_id)
                               .join(UserCommunity, Profile.user_id == UserCommunity.user_id)
                               .join(Community, UserCommunity.community_id == Community.id)
                               .join(Partition, Community.partition_id == Partition.id)
                               .join(Graph, Partition.graph_id == Graph.id)
                               .join(UserContext, and_(Graph.context_id == UserContext.context_id,
                                     User.id == UserContext.user_id))
                               .filter(User.id.in_(active_users)).statement,
                               con=session.bind, index_col='id')

        data['topical_attachment'] = self.__min_max(data['topical_attachment'])

        rank = data.groupby(['id', 'user_name'])\
            .apply(lambda x: abs(x['follower_rank'].head(1) - 1) *
                             (x['topical_attachment'].sum() + x['indegree_centrality'].sum()))\
            .reset_index(level=[0, 1]).rename(columns={'follower_rank': 'rank'})\
            .sort_values(by=['rank', 'user_name'], ascending=[False, True])
        rank.reset_index(drop=True, inplace=True)

        self.datasources.files.write(rank, 'ranking', 'rank_2', 'rank_2', 'csv')

    def __rank_3(self):
        active_users = self.datasources.files.read('ranking', 'get_active_users', 'active_users', 'csv').index.tolist()

        with self.datasources.database.session_scope() as session:
            data = pd.read_sql(session.query(User.id, User.user_name,
                                             Profile.follower_rank,
                                             UserContext.topical_attachment,
                                             UserCommunity.indegree_centrality)
                               .join(Profile, User.id == Profile.user_id)
                               .join(UserCommunity, Profile.user_id == UserCommunity.user_id)
                               .join(Community, UserCommunity.community_id == Community.id)
                               .join(Partition, Community.partition_id == Partition.id)
                               .join(Graph, Partition.graph_id == Graph.id)
                               .join(UserContext, and_(Graph.context_id == UserContext.context_id,
                                     User.id == UserContext.user_id))
                               .filter(User.id.in_(active_users)).statement,
                               con=session.bind, index_col='id')

        data['topical_attachment'] = self.__min_max(data['topical_attachment'])

        rank = data.groupby(['id', 'user_name'])\
            .apply(lambda x: abs(x['follower_rank'].head(1) - 1) *
                             (x['topical_attachment'].sum() + 1 / (x['indegree_centrality'].sum() + 1)))\
            .reset_index(level=[0, 1]).rename(columns={'follower_rank': 'rank'})\
            .sort_values(by=['rank', 'user_name'], ascending=[False, True])
        rank.reset_index(drop=True, inplace=True)

        self.datasources.files.write(rank, 'ranking', 'rank_3', 'rank_3', 'csv')
