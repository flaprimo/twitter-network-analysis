import pandas as pd
import logging

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
import helper
from datasources import PipelineIO
from datasources.database.database import db
from datasources.database.model import User, Event, UserEvent, UserCommunity, Profile

logger = logging.getLogger(__name__)


class Ranking:
    def __init__(self, config, stage_input=None, stage_input_format=None):
        self.config = config
        self.input = PipelineIO.load_input([], stage_input, stage_input_format)
        self.output_prefix = 'r'
        self.output_format = {
            'rank': {
                'type': 'pandas',
                'path': self.config.get_path(self.output_prefix, 'rank'),
                'r_kwargs': {
                    'dtype': {
                        'user_id': 'uint32',
                        'user_name': str,
                        'rank': 'float32'
                    }
                },
                'w_kwargs': {'index': False}
            }
        }
        self.output = PipelineIO.load_output(self.output_format)
        logger.info(f'INIT for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC for {self.config.dataset_name}')

        if self.config.skip_output_check or not self.output:
            self.output['rank'] = self.__ranking()

            if self.config.save_db_output:
                self.__persist_rank(self.output['rank'])

            if self.config.save_io_output:
                PipelineIO.save_output(self.output, self.output_format)

        logger.info(f'END for {self.config.dataset_name}')

        return self.output, self.output_format

    @staticmethod
    def __ranking():
        logger.info('getting tw stream for users')

        # number of communities each user is in * indegree centrality (weight)

        with db.session_scope() as session:
            # users = session.query(User).all()

            # number of communities each user has been, sum of the indegree centralities for each user
            communities_per_user = session.query(UserCommunity.community_id,
                                                 func.count(UserCommunity.community_id),
                                                 func.sum(UserCommunity.indegree_centrality))\
                .group_by(UserCommunity.user_id).all()

            # number of events each user has been
            events_per_user = session.query(UserEvent.event_id, func.count(UserEvent.event_id))\
                .group_by(UserEvent.user_id).all()

            # follower_rank (popularity)
            popularity = session.query(Profile.user_id, Profile.follower_rank).all()



            # r = session.query(func.count(models.Child.id), User) \
            #     .select_from(User).outerjoin(models.Child).group_by(User.id)

            # r = session.query(func.count(Communities.id), models.Parent) \
            #     .select_from(models.Parent).join(models.Child).group_by(models.Parent.id)

        return df_stream

    @staticmethod
    def __persist_rank(rank):
        logger.info('persist userevent metrics')

        user_names = rank['user_name'].tolist()

        try:
            with db.session_scope() as session:
                # get all users for current dataset
                user_entities = session.query(User)\
                    .filter(User.user_name.in_(user_names)).all()

                # get current event
                event_entity = session.query(Event).filter(Event.name == dataset_name).first()

                profile_entities = []
                for p in profile_records:
                    # get user entities and profile info
                    user_entity = next(filter(lambda x: x.user_name == p['user_name'], user_entities), None)
                    profile = {k: p[k] for k in ('topical_attachment', 'event_focus', 'topical_strength')}

                    # create profile entity
                    profile_entity = UserEvent(**profile, user=user_entity, event=event_entity)
                    profile_entities.append(profile_entity)

                session.add_all(profile_entities)
            logger.debug('userevent info successfully persisted')
        except IntegrityError:
            logger.debug('userevent info already exists or constraint is violated and could not be added')
