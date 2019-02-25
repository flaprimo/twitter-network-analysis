from TwitterAPI import TwitterAPI, TwitterPager
import logging

logger = logging.getLogger(__name__)


class TwApi:
    def __init__(self, consumer_key, consumer_key_secret, access_token, access_token_secret):
        self.consumerKey = consumer_key
        self.consumerSecret = consumer_key_secret
        self.accessKey = access_token
        self.accessSecret = access_token_secret

        self.label = 'prod'
        logger.debug('INIT Tw api')

    def __get_api(self):
        api = TwitterAPI(self.consumerKey, self.consumerSecret, self.accessKey, self.accessSecret, auth_type='oAuth2')

        return api

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

    def premium_search(self, product='fullarchive', query='', since=None, until=None, n=0):  # also product='30day'
        logger.info(f'tw api search for: {query}')

        api = self.__get_api()
        search = TwitterPager(api, f'tweets/search/{product}/:{self.label}',
                              {'query': query,
                               'fromDate': since.strftime('%Y%m%d%H%M'),
                               'toDate': until.strftime('%Y%m%d%H%M')})

        return self.__get_tweets(search, n)

    def get_user_timeline(self, user_name, n=0):
        logger.info(f'tw api timeline for user: {user_name}')

        api = self.__get_api()
        search = TwitterPager(api, 'statuses/user_timeline',
                              {'screen_name': user_name, 'count': n, 'exclude_replies': True})

        return self.__get_tweets(search, n)
