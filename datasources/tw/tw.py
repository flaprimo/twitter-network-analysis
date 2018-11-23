from datasources.tw.helper import query_builder
from datasources.tw.proxy_provider import ProxyProvider
from datasources.tw.tw_static_scraper import TwStaticScraper
from datasources.tw.tw_dynamic_scraper import TwDynamicScraper
from datasources.tw.tw_api import TwApi
import logging
import json
import os

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class Tw:
    def __init__(self, config_file_path='config.json'):
        # configure paths
        current_path = os.path.dirname(__file__)
        config_file_path = os.path.join(current_path, config_file_path)
        cache_path = os.path.join(current_path, 'cache/')
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)

        # load configs
        self.configs = Tw.load_config(config_file_path)

        # load tw accesses
        self.proxy_provider = ProxyProvider(self.configs['proxy_provider']['base_url'], cache_path + 'proxy_list.json')
        self.tw_static_scraper = TwStaticScraper(self.configs['tw_scraper']['base_url'], self.proxy_provider)
        self.tw_dynamic_scraper = TwDynamicScraper(self.configs['tw_scraper']['base_url'], self.proxy_provider)
        # self.tw_api = TwApi(
        #     self.configs['tw_api']['consumer_key'],
        #     self.configs['tw_api']['consumer_key_secret'],
        #     self.configs['tw_api']['access_token'],
        #     self.configs['tw_api']['access_token_secret'],
        # )
        logger.info('INIT Tw')

    @staticmethod
    def load_config(file_path):
        logger.info('load config file')
        with open(file_path) as json_file:
            configs = json.load(json_file)

        return configs


# TESTS!
def main():
    tw = Tw()

    # complex query example
    # query = query_builder(
    #     '#DataScience AND #healthcare',
    #     people={'from': 'pmissier'},
    #     date={'since': '2018-11-05', 'until': '2018-11-08'})
    #
    # q = tw.tw_dynamic_scraper.search(query, n=100)

    hashtags = ['#GTC18', '#IPAW2018', '#NIPS2017', '#provenanceweek', '#TCF2018', '#ECMLPKDD2018',
                '#emnlp2018', '#kdd', '#msignite2018', '#ona18', '#recsys']
    queries = [query_builder(h) for h in hashtags]

    results = [tw.tw_dynamic_scraper.search(q, n=100) for q in queries]
    for h, r in zip(hashtags, results):
        print(f'{h} -> {len(r)} tws collected')

    # profile1 = tw.tw_static_scraper.get_profile('pmissier')
    # follower_rank1 = profile1['followers'] / (profile1['followers'] + profile1['following'])
    #
    # print(f'profile: {profile1}')
    # print(f'follower_rank: {follower_rank1}')

    # q = [(query_builder('#kdd')),
    #      query_builder('#datainequality',
    #                    people={'from': 'pmissier'},
    #                    date={'since': '2018-10-24', 'until': '2018-10-25'})
    #      ]
    #
    # for i in q:
    #     print(f'{tw.tw_dynamic_scraper.base_url}?{i}')


if __name__ == "__main__":
    main()
