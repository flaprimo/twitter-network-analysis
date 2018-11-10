from lxml import html
import requests
import requests_cache
import os

requests_cache.install_cache(cache_name=os.path.join(os.path.dirname(__file__), 'cache/twitter_static_scraper_cache'),
                             expire_after=2.628e+6)


class TwStaticScraper:
    def __init__(self, base_url, proxy_provider):
        self.proxy_provider = proxy_provider
        self.base_url = base_url

    def get_profile(self, user_name):
        # get proxy
        proxy_ip, proxy_port, proxy_https = self.proxy_provider.get_proxy()
        proxy_url = f'{proxy_ip}:{proxy_port}'
        proxy = {'https': proxy_url} if proxy_https else {'http': proxy_url}

        response = requests.get(url=self.base_url + user_name, proxies=proxy)

        # parse queried profile
        if response.ok:
            root = html.fromstring(response.content)
            profile = {}

            # get profile stats
            profile_nav = root.xpath('//ul[@class="ProfileNav-list"]')[0]
            profile_nav_path = './li[contains(@class, "ProfileNav-item--{0}")]/' \
                               'a/span[@class="ProfileNav-value"]/@data-count'
            profile['tweets'] = int(profile_nav.xpath(profile_nav_path.format('tweets'))[0])
            profile['following'] = int(profile_nav.xpath(profile_nav_path.format('following'))[0])
            profile['followers'] = int(profile_nav.xpath(profile_nav_path.format('followers'))[0])
            profile['likes'] = int(profile_nav.xpath(profile_nav_path.format('favorites'))[0])

            # get profile personal info
            profile_header_card = root.xpath('//div[@class="ProfileHeaderCard"]')[0]
            profile['name'] = profile_header_card.xpath('./h1[@class="ProfileHeaderCard-name"]/a/text()')[0]
            profile['bio'] = profile_header_card.xpath('./p[contains(@class, "ProfileHeaderCard-bio")]/text()')[0]
            profile['location'] = profile_header_card \
                .xpath('./div[@class="ProfileHeaderCard-location "]/span[2]/text()')[0].strip()
            profile['url'] = profile_header_card \
                .xpath('./div[@class="ProfileHeaderCard-url "]/span[2]/a/@title')[0]
            profile['join_date'] = profile_header_card \
                .xpath('./div[@class="ProfileHeaderCard-joinDate"]/span[2]/text()')[0].strip('Joined ')

            return profile
        else:
            response.raise_for_status()
