from datetime import datetime
from lxml import html
import requests
import requests_cache
import logging
import os
from requests import Timeout
from requests.exceptions import ProxyError

logger = logging.getLogger(__name__)
requests_cache.install_cache(cache_name=os.path.join(os.path.dirname(__file__), 'cache/twitter_static_scraper_cache'),
                             expire_after=2.628e+6)


class TwStaticScraper:
    def __init__(self, base_url, proxy_provider):
        self.proxy_provider = proxy_provider
        self.base_url = base_url
        self.timeout = 2

    def __get_page(self, url, timeout=2):
        response = None

        while not response:
            # get proxy
            logger.debug('getting proxy')
            proxy_ip, proxy_port, proxy_https, proxy_code = self.proxy_provider.get_proxy()
            proxy_url = f'{proxy_ip}:{proxy_port}'
            proxy = {'https': proxy_url} if proxy_https else {'http': proxy_url}

            try:
                logger.debug(f'url to query: {url}')
                response = requests.get(url=url, proxies=proxy, timeout=timeout)
                logger.debug(f'from cache: {response.from_cache}')
                if not response.ok:
                    logger.debug('failed getting user')
                    response.raise_for_status()
            except Timeout:
                logger.debug(f'proxy {proxy_ip}:{proxy_port} too slow, retrying')
            except ProxyError:
                logger.debug(f'cannot connect to proxy {proxy_ip}:{proxy_port}, retrying')

        return response

    def get_user(self, user_name):
        logger.info('getting user')

        url = self.base_url + user_name + '?lang=en-gb'
        logger.debug(f'url to query for "{user_name}": {url}\n')
        response = self.__get_page(url)

        # parse queried profile
        logger.debug('user fetched')
        logger.debug('analyzing user')

        root = html.fromstring(response.content)
        user = {
            'user_name': user_name
        }

        # get profile stats
        profile_nav = root.xpath('//ul[@class="ProfileNav-list"]')[0]
        profile_nav_path = './li[contains(@class, "ProfileNav-item--{0}")]/' \
                           'a/span[@class="ProfileNav-value"]/@data-count'
        user['tweets'] = int(profile_nav.xpath(profile_nav_path.format('tweets'))[0])
        user['following'] = int(profile_nav.xpath(profile_nav_path.format('following'))[0])
        user['followers'] = int(profile_nav.xpath(profile_nav_path.format('followers'))[0])
        user['likes'] = int(profile_nav.xpath(profile_nav_path.format('favorites'))[0])

        # get profile personal info
        profile_header_card = root.xpath('//div[@class="ProfileHeaderCard"]')[0]
        user['name'] = profile_header_card.xpath('./h1[@class="ProfileHeaderCard-name"]/a/text()')[0]
        try:
            user['bio'] = profile_header_card.xpath('./p[contains(@class, "ProfileHeaderCard-bio")]/text()')[0]
        except IndexError:
            user['bio'] = ''
        try:
            user['location'] = profile_header_card \
                .xpath('./div[@class="ProfileHeaderCard-location "]/span[2]/text()')[0].strip()
        except IndexError:
            user['location'] = ''
        try:
            user['url'] = profile_header_card \
                .xpath('./div[@class="ProfileHeaderCard-url "]/span[2]/a/@title')[0]
        except IndexError:
            user['url'] = ''
        join_date = profile_header_card \
            .xpath('./div[@class="ProfileHeaderCard-joinDate"]/span[2]/text()')[0].replace('Joined ', '')
        user['join_date'] = datetime.strptime(join_date, '%B %Y').date()

        logger.debug(f'user obtained for "{user_name}": {user}\n')

        return user
