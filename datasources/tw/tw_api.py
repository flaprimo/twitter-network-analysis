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

    @staticmethod
    def __get_tweets(search, n):
        tw_list = []
        for i, tw_json in enumerate(search.get_iterator(), start=1):
            if 'message' in tw_json:
                logger.debug(f'{tw_json["message"]} ({tw_json["code"]})')
            else:
                tw_list.append(tw_json)

            if i >= n:
                break

        return tw_list

    def premium_search(self, product='fullarchive', label='prod', query='', since=None, until=None, n=100):
        logger.info(f'tw api search for: {query}')

        search = TwitterPager(self.api, f'tweets/search/{product}/:{label}',
                              {'query': query,
                               'fromDate': since.strftime('%Y%m%d%H%M'),
                               'toDate': until.strftime('%Y%m%d%H%M')})

        return self.__get_tweets(search, n)

    def get_user_timeline(self, user_name, n=200):
        logger.info(f'tw api timeline for user: {user_name}')

        with requests_cache.enabled(self.cache_path, expire_after=86400):
            user_timeline = self.api.request('statuses/user_timeline',
                                             {'screen_name': user_name, 'count': n, 'exclude_replies': 'true'})
        return user_timeline

    def get_user_timelines(self, user_name_list, n=200):
        logger.info(f'tw api timeline for {len(user_name_list)} users')

        stream = []
        for u in user_name_list:
            user_stream = self.get_user_timeline(u, n)
            stream.append({'user_name': u, 'stream': user_stream.json()})
            time.sleep(2)

        return stream

    def get_rate_limit_status(self, resources):
        rate_limit_status = self.api.request('application/rate_limit_status', {'resources': resources})

        return rate_limit_status
