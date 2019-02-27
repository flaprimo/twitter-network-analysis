from datetime import datetime
from lxml import html
import requests
import requests_cache
import logging
from requests import Timeout
from requests.exceptions import ProxyError, SSLError, HTTPError, ConnectionError

logger = logging.getLogger(__name__)


class TwStaticScraper:
    def __init__(self, base_url, proxy_provider, cache_path='tw_static_scraper_cache'):
        self.proxy_provider = proxy_provider
        self.base_url = base_url
        self.cache_path = cache_path
        self.timeout = 2

    def __get_page(self, url):
        response = None

        while not response:
            # get proxy
            logger.debug('getting proxy')
            proxy_ip, proxy_port, proxy_https, proxy_code = self.proxy_provider.get_proxy()
            proxy_url = f'{proxy_ip}:{proxy_port}'
            proxy = {'https': proxy_url} if proxy_https else {'http': proxy_url}
            logger.debug(f'url to query: {url}')

            try:
                with requests_cache.enabled(self.cache_path, expire_after=2.628e+6):
                    response = requests.get(url=url, proxies=proxy, timeout=self.timeout)
                    logger.debug(f'from cache: {response.from_cache}')
                    if not response.ok:
                        logger.debug('failed getting user')
                        response.raise_for_status()
            except Timeout:
                logger.debug(f'proxy {proxy_ip}:{proxy_port} too slow, retrying')
            except HTTPError:
                logger.debug(f'user doesn\'t exist anymore')
                raise HTTPError
            except (ProxyError, SSLError, ConnectionError):
                logger.debug(f'cannot connect to proxy {proxy_ip}:{proxy_port}, retrying')

        return response

    def get_user(self, user_name):
        logger.info('getting user')

        url = self.base_url + user_name + '?lang=en-gb'
        logger.debug(f'url to query for "{user_name}": {url}')

        try:
            response = self.__get_page(url)

            # parse queried profile
            logger.debug('user fetched')
            logger.debug('analyzing user')

            root = html.fromstring(response.content)

            # get profile stats
            profile_nav = root.xpath('//ul[@class="ProfileNav-list"]')[0]
            profile_nav_path = './li[contains(@class, "ProfileNav-item--{0}")]/' \
                               'a/span[@class="ProfileNav-value"]/@data-count'
            profile_header = root.xpath('//div[@class="ProfileHeaderCard"]')[0]

            user = {
                'user_name': user_name,
                'tweets': int(next(iter(profile_nav.xpath(profile_nav_path.format('tweets'))), 0)),
                'following': int(next(iter(profile_nav.xpath(profile_nav_path.format('following'))), 0)),
                'followers': int(next(iter(profile_nav.xpath(profile_nav_path.format('followers'))), 0)),
                'likes': int(next(iter(profile_nav.xpath(profile_nav_path.format('favorites'))), 0)),

                'name': next(iter(profile_header.xpath('./h1[@class="ProfileHeaderCard-name"]/a/text()')), ''),
                'bio': next(iter(
                    profile_header.xpath('./p[contains(@class, "ProfileHeaderCard-bio")]/text()')), ''),
                'location': next(iter(
                    profile_header.xpath('./div[@class="ProfileHeaderCard-location "]/span[2]/text()')), '').strip(),
                'url': next(iter(
                    profile_header.xpath('./div[@class="ProfileHeaderCard-url "]/span[2]/a/@title')), ''),
                'join_date': datetime.strptime(
                    profile_header.xpath('./div[@class="ProfileHeaderCard-joinDate"]/span[2]/text()')[0]
                    .replace('Joined ', ''), '%B %Y').date()
                if len(profile_header.xpath('./div[@class="ProfileHeaderCard-joinDate"]')) > 0
                else datetime.now().date()
            }
            logger.debug(f'user obtained for "{user_name}": {user}\n')

            return user

        except HTTPError:
            logger.debug(f'user "{user_name}" doesn\'t exist anymore, skipping\n')

        except IndexError:
            logger.debug(f'user "{user_name}" likely suspended, skipping\n')
