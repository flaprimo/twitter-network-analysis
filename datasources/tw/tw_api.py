import requests_cache
from TwitterAPI import TwitterAPI, TwitterPager
import time
import logging

logger = logging.getLogger(__name__)


class TwApi:
    def __init__(self, consumer_key, consumer_key_secret, access_token, access_token_secret, cache_path='tw_api'):
        self.cache_path = cache_path
        self.api = TwitterAPI(consumer_key, consumer_key_secret, access_token, access_token_secret,
                              auth_type='oAuth2')
        logger.debug('INIT Tw api')

    def __get_tweets(self, pager, n, wait=5):
        tw_list = []

        with requests_cache.enabled(self.cache_path, expire_after=86400):
            for i, tw in zip(range(n), pager.get_iterator(wait=wait)):
                if 'message' in tw:
                    logger.debug(f'{tw["message"]} ({tw["code"]})')
                else:
                    tw_list.append(tw)

        return tw_list

    def premium_search(self, product='fullarchive', label='prod', query='', since=None, until=None, n=100):
        logger.info(f'tw api search for: {query}')

        pager = TwitterPager(self.api, f'tweets/search/{product}/:{label}',
                             {'query': query,
                              'fromDate': since.strftime('%Y%m%d%H%M'),
                              'toDate': until.strftime('%Y%m%d%H%M')})

        return self.__get_tweets(pager, n)

    def get_user_timeline(self, user_name, n=200):
        logger.info(f'tw api timeline for user: {user_name}')

        pager = TwitterPager(self.api, 'statuses/user_timeline',
                             {'screen_name': user_name,
                              'count': n,
                              'exclude_replies': 'true'})

        return self.__get_tweets(pager, n, 2)

    def get_user_timelines(self, user_name_list, n=200):
        logger.info(f'tw api timeline for {len(user_name_list)} users')

        stream = []
        for u in user_name_list:
            user_stream = self.get_user_timeline(u, n)
            if user_stream:
                stream.append({'user_name': u, 'stream': user_stream})
            time.sleep(2)

        return stream

    def get_user_profiles(self, user_name_list):
        logger.info(f'tw api profiles for {len(user_name_list)} users')

        # group usernames in 100 lists
        u_groups = [user_name_list[n:n + 100] for n in range(0, len(user_name_list), 100)]

        stream = []
        for u_list in u_groups:
            with requests_cache.enabled(self.cache_path, expire_after=86400):
                u_stream = self.api.request('users/lookup', {'screen_name': u_list, 'include_entities': 'false'})
            stream.extend(u_stream)
            time.sleep(3)

        return stream

    def get_rate_limit_status(self, resources):
        rate_limit_status = self.api.request('application/rate_limit_status', {'resources': resources}).json()

        return rate_limit_status
