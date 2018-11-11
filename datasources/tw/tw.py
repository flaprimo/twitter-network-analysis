from datasources.tw.proxy_provider import ProxyProvider
from datasources.tw.tw_static_scraper import TwStaticScraper
from datasources.tw.tw_dynamic_scraper import TwDynamicScraper
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

        # load tw apis
        self.proxy_provider = ProxyProvider(self.configs['proxy_provider']['base_url'], cache_path + 'proxy_list.json')
        self.tw_static_scraper = TwStaticScraper(self.configs['tw_scraper']['base_url'], self.proxy_provider)
        self.tw_dynamic_scraper = TwDynamicScraper(self.configs['tw_scraper']['base_url'], self.proxy_provider)
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

    # query example
    # hashtags = '#DataScience AND #healthcare'
    # other_params = {
    #     'from': 'pmissier',
    #     'since': '2018-11-05',
    #     'until': '2018-11-08'
    # }
    # q = twitter_dynamic_scraper.query(hashtags=hashtags, other_params=other_params)

    # hashtags = '#kdd'
    # q = tw.tw_dynamic_scraper.query(hashtags)
    #
    # print(q)

    profile1 = tw.tw_static_scraper.get_profile('pmissier')
    follower_rank1 = profile1['followers'] / (profile1['followers'] + profile1['following'])

    print(f'profile: {profile1}')
    print(f'follower_rank: {follower_rank1}')


if __name__ == "__main__":
    main()
