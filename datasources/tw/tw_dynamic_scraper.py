from selenium import webdriver
from urllib.parse import quote
import logging

logger = logging.getLogger(__name__)


class TwDynamicScraper:
    def __init__(self, base_url, proxy_provider):
        self.proxy_provider = proxy_provider
        self.base_url = base_url + 'search'

    def query(self, hashtags, other_params=None, language=''):
        logger.info('getting query')
        # get query url
        query_url = self.__get_query_url(hashtags, other_params, language)

        # get proxy
        logger.debug('getting proxy')
        proxy_ip, proxy_port, proxy_https = self.proxy_provider.get_proxy()

        # set selenium options
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            'enable_do_not_track': True,
            'profile.default_content_setting_values.cookies': 2,
            'profile.managed_default_content_settings.images': 2,
            'disk-cache-size': 4096
        }
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument(f'--proxy-server={proxy_ip}:{proxy_port}')

        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.set_window_position(0, 0)
        driver.set_window_size(1024, 768)

        # load queried web page
        driver.get(query_url)

        logger.debug('query results fetched')
        logger.debug('analyzing query results')
        twitter_stream = driver.find_element_by_id('stream-items-id')
        print(twitter_stream.get_attribute('innerHTML'))

        driver.quit()

        return {}

    def __get_query_url(self, hashtags, other_params=None, language=''):
        hashtags = quote(hashtags, safe='')
        if other_params:
            params = ' '.join(f'{name}:{value}' for name, value in other_params.items())
            params = quote(' ' + params, safe='')
        else:
            params = ''

        query = f'{self.base_url}?l={language}&q={hashtags}{params}'

        logger.debug(f'composed url query: {query}')

        return query
