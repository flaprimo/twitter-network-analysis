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
        # scrapers
        self.proxy_provider = ProxyProvider(self.configs['proxy_provider']['base_url'], cache_path + 'proxy_list.json')
        self.tw_static_scraper = TwStaticScraper(self.configs['tw_scraper']['base_url'], self.proxy_provider,
                                                 cache_path + 'tw_static_scraper_cache')
        self.tw_dynamic_scraper = TwDynamicScraper(self.configs['tw_scraper']['base_url'], self.proxy_provider)

        # apis
        self.tw_api = TwApi(
            self.configs['tw_api']['consumer_key'],
            self.configs['tw_api']['consumer_key_secret'],
            self.configs['tw_api']['access_token'],
            self.configs['tw_api']['access_token_secret'],
            cache_path + 'tw_api_cache'
        )
        logger.info('INIT Tw')

    @staticmethod
    def load_config(file_path):
        logger.info('load config file')
        with open(file_path) as json_file:
            configs = json.load(json_file)

        return configs


tw = Tw()
