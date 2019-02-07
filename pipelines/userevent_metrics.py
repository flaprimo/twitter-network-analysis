import logging
import pandas as pd
from sqlalchemy.exc import IntegrityError

from datasources.database.model import UserEvent, Event, User
from datasources.tw.helper import query_builder
from datasources.tw.tw import tw
from .pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class UserEventMetrics(PipelineBase):
    def __init__(self, datasources, file_prefix):
        files = [
            {
                'stage_name':  'get_user_stream',
                'file_name':  'stream',
                'file_extension':  'csv',
                'r_kwargs': {
                    'dtype': {
                        'tw_id': str,
                        'user_id': 'uint32',
                        'author': str,
                        'date': str,
                        'language': str,
                        'text': str,
                        'no_replies': 'uint32',
                        'no_retweets': 'uint32',
                        'no_likes': 'uint32'
                    },
                    'converters': {
                        'reply': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'hashtags': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'emojis': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'urls': lambda x: x.strip('[]').replace('\'', '').split(', '),
                        'mentions': lambda x: x.strip('[]').replace('\'', '').split(', ')
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            },
            {
                'stage_name': 'compute_metrics',
                'file_name': 'userevent_metrics',
                'file_extension': 'csv',
                'r_kwargs': {
                    'dtype': {
                        # 'user_id': 'uint32',
                        'user_name': str,
                        'topical_attachment': 'float32',
                        'topical_focus': 'float32',
                        'topical_strength': 'float32'
                    }
                },
                'w_kwargs': {
                    'index': False
                }
            }
        ]
        tasks = [self.__get_user_stream]
        super(UserEventMetrics, self).__init__('userevent_metrics', files, tasks, datasources, file_prefix)

    def __get_user_stream(self):
        if not self.datasources.files.exists(
                'userevent_metrics', 'get_user_stream', 'stream', 'csv', self.context_name):
            profile_info = self.datasources.files.read(
                'profile_metrics', 'profile_info', 'profile_info', 'csv', self.context_name)

            user_names = profile_info['user_name'].tolist()
            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.reset_index().to_dict('records')[0]

            streams = []
            for u in user_names:
                query = query_builder(
                    people={'from': u},
                    date={
                        'since': context_record['start_date'],
                        'until': context_record['end_date']
                    })
                stream = tw.tw_dynamic_scraper.search(query)

                # only keeps tws from current user_name
                stream = [s for s in stream if s['author'] == u]

                streams.extend(stream)
            # can try merging for having users_id and filtering proper users
            df_stream = pd.DataFrame.from_records(streams)

            self.datasources.files.write(
                df_stream, 'userevent_metrics', 'get_user_stream', 'stream', 'csv', self.context_name)

    def __compute_metrics(self):
        if not self.datasources.files.exists(
                'userevent_metrics', 'compute_metrics', 'userevent_metrics', 'csv', self.context_name):
            stream = self.datasources.files.read(
                'userevent_metrics', 'get_user_stream', 'stream', 'csv', self.context_name)
            nodes = self.datasources.files.read(
                'profile_metrics', 'remove_nonexistent_users', 'nodes', 'csv', self.context_name)

            context = self.datasources.contexts.get_context(self.context_name)
            context_record = context.reset_index().to_dict('records')[0]
            hashtags = context_record['hashtags'][0]

            stream['tw_ontopic'] = stream['hashtags'].apply(lambda t: any(h in hashtags for h in t))
            stream['link_ontopic'] = stream[stream['tw_ontopic']]['urls'].apply(lambda t: t != ['']).fillna(False)

            # topical attachment
            def topical_attachment_alg(tw_ontopic, tw_offtopic, link_ontopic, link_offtopic):
                return (tw_ontopic + link_ontopic) / (tw_offtopic + link_offtopic + 1)

            topical_attachment = \
                stream[['author', 'tw_ontopic', 'link_ontopic']].groupby('author') \
                    .apply(lambda x: topical_attachment_alg(x['tw_ontopic'].sum(), (~x['tw_ontopic']).sum(),
                                                            x['link_ontopic'].sum(), (~x['link_ontopic']).sum())) \
                    .to_frame().rename(columns={0: 'topical_attachment'})

            # topical focus
            def topical_focus_alg(t_ontopic, t_offtopic):
                return t_ontopic / (t_offtopic + 1)

            topical_focus = \
                stream[['author', 'tw_ontopic']].groupby('author') \
                    .apply(lambda x: topical_focus_alg(x['tw_ontopic'].sum(), (~x['tw_ontopic']).sum())) \
                    .to_frame().rename(columns={0: 'topical_focus'})

            # topical strength
            def topical_strength_alg(link_ontopic, link_offtopic, rtw_ontopic, rtw_offtopic):
                import math
                return (link_ontopic * math.log10(link_ontopic + rtw_ontopic + 1)) / \
                       (link_offtopic * math.log10(link_offtopic + rtw_offtopic + 1) + 1)

            topical_strength = \
                stream[['author', 'tw_ontopic', 'link_ontopic', 'no_retweets']].groupby('author') \
                    .apply(lambda x: topical_strength_alg(x['link_ontopic'].sum(), (~x['link_ontopic']).sum(),
                                                          x[x['tw_ontopic']]['no_retweets'].sum(),
                                                          x[~x['tw_ontopic']]['no_retweets'].sum())) \
                    .to_frame().rename(columns={0: 'topical_strength'})

            userevents = topical_attachment \
                .merge(topical_focus, left_index=True, right_index=True) \
                .merge(topical_strength, left_index=True, right_index=True) \
                .reset_index().rename(columns={'author': 'user_name'})

            # add missing nodes
            userevents = userevents.merge(nodes[['user_name']], left_on='user_name', right_on='user_name',
                                          how='outer', sort='True').fillna(0)

            userevent_records = userevents.set_index('user_name').to_dict('index')

            try:
                with self.datasources.database.session_scope() as session:
                    # get all users for current dataset
                    user_entities = session.query(User) \
                        .filter(User.user_name.in_(userevent_records.keys())).all()

                    # get current event
                    event_entity = session.query(Event).filter(Event.name == self.context_name).first()

                    userevent_entities = []
                    for user_name, metrics in userevent_records.items():
                        # get user entities and userevent info
                        user_entity = next(filter(lambda x: x.user_name == user_name, user_entities), None)

                        # create userevent entity
                        userevent_entity = UserEvent(**metrics, user=user_entity, event=event_entity)
                        userevent_entities.append(userevent_entity)

                    session.add_all(userevent_entities)
                logger.debug('userevent info successfully persisted')
            except IntegrityError:
                logger.debug('userevent info already exists or constraint is violated and could not be added')

            self.datasources.files.write(
                userevents, 'userevent_metrics', 'compute_metrics', 'userevent_metrics', 'csv', self.context_name)
