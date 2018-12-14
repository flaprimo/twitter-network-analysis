from TwitterAPI import TwitterAPI, TwitterPager
import logging

logger = logging.getLogger(__name__)


class TwPremiumApi:
    def __init__(self, consumer_key, consumer_key_secret, access_token, access_token_secret):
        self.consumerKey = consumer_key
        self.consumerSecret = consumer_key_secret
        self.accessKey = access_token
        self.accessSecret = access_token_secret

        self.product = '30day'# 'fullarchive'
        self.label = 'prod'
        logger.debug('INIT Tw premium api')

    def __get_api(self):
        api = TwitterAPI(self.consumerKey, self.consumerSecret, self.accessKey,  self.accessSecret)

        return api

    def create_search(self, query='', since=None, until=None, n=0):
        logger.info(f'tw api search for: {query}')

        api = self.__get_api()
        search = TwitterPager(api, f'tweets/search/{self.product}/:{self.label}',
                              {'query': query,
                               'fromDate': since.strftime('%Y%m%d%H%M'),
                               'toDate': until.strftime('%Y%m%d%H%M')})

        tw_list = []
        for i, tw_json in enumerate(search.get_iterator(), start=1):
            if 'message' in tw_json:
                logger.debug(f'{tw_json["message"]} ({tw_json["code"]})')
            else:
                tw_list.append(tw_json)

            if i >= n:
                break

        return tw_list
