import logging
from sqlalchemy.exc import IntegrityError
from datasources.database import User, Profile, Context, Graph, Partition, Community, UserCommunity, UserContext
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class Persistence(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = []
        tasks = [self.__add_context, self.__add_graph, self.__add_partition, self.__add_communities, self.__add_users,
                 self.__add_user_communities, self.__add_user_context]
        super(Persistence, self).__init__('persistence', files, tasks, datasources, file_prefix)

    def __add_context(self):
        context = self.datasources.files.read(
            'context_detection', 'create_context', 'context', 'csv', self.context_name)
        context_record = context.reset_index().to_dict('records')[0]
        context_record['hashtags'] = ' '.join(context_record['hashtags'])

        try:
            with self.datasources.database.session_scope() as session:
                context_entity = Context(**context_record)
                session.add(context_entity)
            logger.debug('context successfully persisted')
        except IntegrityError:
            logger.debug('context already exists or constraint is violated and could not be added')

    def __add_graph(self):
        graph_summary = self.datasources.files.read(
            'network_metrics', 'graph_summary', 'graph_summary', 'csv', self.context_name)
        graph_record = graph_summary.to_dict('records')[0]

        try:
            with self.datasources.database.session_scope() as session:
                context_entity = session.query(Context).filter(Context.name == self.context_name).first()
                graph_entity = Graph(**graph_record, context=context_entity)
                session.add(graph_entity)
            logger.debug('graph successfully persisted')
        except IntegrityError:
            logger.debug('graph already exists or constraint is violated and could not be added')

    def __add_partition(self):
        partition = self.datasources.files.read(
            'community_detection_metrics', 'pquality', 'pquality', 'csv', self.context_name)
        partition_record = partition[['avg']].T.to_dict('records')[0]

        try:
            with self.datasources.database.session_scope() as session:
                graph_entity = session.query(Graph).join(Graph.context) \
                    .filter(Context.name == self.context_name).first()
                partition_entity = Partition(**partition_record, graph=graph_entity)
                session.add(partition_entity)
            logger.debug('partition successfully persisted')
        except IntegrityError:
            logger.debug('partition already exists or constraint is violated and could not be added')

    def __add_communities(self):
        partition_summary = self.datasources.files.read(
            'community_detection_metrics', 'partition_summary', 'partition_summary', 'csv', self.context_name)
        communities = [{'name': c} for c in partition_summary.index.tolist()]

        try:
            with self.datasources.database.session_scope() as session:
                partition_entity = session.query(Partition).join(Partition.graph).join(Graph.context) \
                    .filter(Context.name == self.context_name).first()

                community_entities = [Community(**c, partition=partition_entity) for c in communities]
                session.add_all(community_entities)
            logger.debug('communities successfully persisted')
        except IntegrityError:
            logger.debug('community already exists or constraint is violated and could not be added')

    def __add_users(self):
        users = self.datasources.files.read(
            'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)

        user_records = users.to_dict('records')
        user_names = users['user_name'].tolist()

        try:
            with self.datasources.database.session_scope() as session:
                # get users and split to insert and to update
                user_entities_toupdate = session.query(User.id, User.user_name)\
                    .filter(User.user_name.in_(user_names)).all()
                users_toupdate = [u[1] for u in user_entities_toupdate]
                users_toinsert = set(user_names) - set(users_toupdate)

                # insert new users
                user_records_toinsert = [u for u in user_records if u['user_name'] in users_toinsert]
                user_entities_toinsert = [User(**u) for u in user_records_toinsert]
                session.add_all(user_entities_toinsert)

                # update old users
                user_records_toupdate =\
                    [dict([u for u in user_records if u['user_name'] == u_username_old][0], **{'id': u_id_old})
                     for u_id_old, u_username_old in user_entities_toupdate]
                session.bulk_update_mappings(User, user_records_toupdate)

            logger.debug('user info successfully persisted')
        except IntegrityError:
            logger.debug('user info already exists or constraint is violated and could not be added')

    # def __add_profiles(self):
    #     profiles = self.datasources.files.read(
    #         'profile_metrics', 'follower_rank', 'profiles', 'csv', self.context_name)
    #
    #     profile_records = profiles.to_dict('records')
    #     user_names = [p['user_name'] for p in profile_records]
    #
    #     try:
    #         with self.datasources.database.session_scope() as session:
    #             # add sample
    #             user_example = session.query(User)\
    #                 .filter(User.user_name == '879caldwell').first()
    #             session.add(Profile(user=user_example, follower_rank=100))
    #
    #             # user_entities = session.query(User) \
    #             #     .filter(User.user_name.in_(user_names)).all()
    #
    #             # user_entities_toupdate = session.query(Profile.id, User) \
    #             #     .join(User.profile).filter(User.user_name.in_(user_names)).all()
    #             # users_toupdate = [u_old.user_name for p_id, u_old in user_entities_toupdate]
    #
    #
    #             # print(user_entities)
    #             #
    #             # update old users
    #             # profile_records_toupdate =\
    #             #     [dict([u for u in profile_records if u['user_name'] == u_old.user_name][0], **{'id': p_id})
    #             #      for p_id, u_old in user_entities_toupdate]
    #             # print(profile_records_toupdate)
    #             # session.bulk_update_mappings(Profile, profile_records_toupdate)
    #
    #             # get all users for current dataset
    #             # user_entities = session.query(User) \
    #             #     .filter(User.user_name.in_(user_names)).all()
    #             #
    #             # profile_entities = []
    #             # for p in profile_records:
    #             #     # get user entities and profile info
    #             #     user_entity = next(filter(lambda x: x.user_name == p['user_name'], user_entities), None)
    #             #
    #             #     # create profile entity
    #             #     profile_entity = Profile(follower_rank=p['follower_rank'], user=user_entity)
    #             #     profile_entities.append(profile_entity)
    #             #
    #             # session.add_all(profile_entities)
    #         logger.debug('profile metrics successfully persisted')
    #     except IntegrityError:
    #         logger.debug('profile metrics already exists or constraint is violated and could not be added')

    def __add_user_context(self):
        usercontexts = self.datasources.files.read(
            'usercontext_metrics', 'compute_metrics', 'usercontext_metrics', 'csv', self.context_name)

        usercontext_records = usercontexts.set_index('user_name').to_dict('index')

        try:
            with self.datasources.database.session_scope() as session:
                # get all users for current dataset
                user_entities = session.query(User) \
                    .filter(User.user_name.in_(usercontext_records.keys())).all()

                # get current context
                context_entity = session.query(Context).filter(Context.name == self.context_name).first()

                usercontext_entities = []
                for user_name, metrics in usercontext_records.items():
                    # get user entities and usercontext info
                    user_entity = next(filter(lambda x: x.user_name == user_name, user_entities), None)

                    # create usercontext entity
                    usercontext_entity = UserContext(**metrics, user=user_entity, context=context_entity)
                    usercontext_entities.append(usercontext_entity)

                session.add_all(usercontext_entities)
            logger.debug('usercontext info successfully persisted')
        except IntegrityError:
            logger.debug('usercontext info already exists or constraint is violated and could not be added')

    def __add_user_communities(self):
        nodes = self.datasources.files.read(
            'profile_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name)
        node_records = nodes.to_dict('records')
        user_names = nodes['user_name'].drop_duplicates().tolist()

        try:
            with self.datasources.database.session_scope() as session:
                # get all commmunities for current dataset partition
                community_entities = session.query(Community) \
                    .join(Community.partition).join(Partition.graph).join(Graph.context) \
                    .filter(Context.name == self.context_name).all()

                # get all users for current dataset
                user_entities = session.query(User) \
                    .filter(User.user_name.in_(user_names)).all()

                usercommunity_entities = []
                for u in node_records:
                    # get user and community entities and usercommunity info
                    community_entity = list(filter(lambda x: x.name == u['community'], community_entities))[0]
                    user_entity = list(filter(lambda x: x.user_name == u['user_name'], user_entities))[0]

                    # create usercommunity entity
                    usercommunity_entity = UserCommunity(indegree=u['indegree'],
                                                         indegree_centrality=u['indegree_centrality'],
                                                         hindex=u['hindex'],
                                                         user=user_entity, community=community_entity)
                    usercommunity_entities.append(usercommunity_entity)
                session.add_all(usercommunity_entities)
        except IntegrityError:
            logger.debug('usercommunity already exists or constraint is violated and could not be added')
