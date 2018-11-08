from lxml import html
import requests
import requests_cache

# requests_cache.install_cache(cache_name='cache/cache', expire_after=2.628e+6)
requests_cache.install_cache(cache_name='cache/twitter_static_scraper_cache')
base_url = 'https://twitter.com/'


class TwitterStaticScraper:
    @staticmethod
    def get_follower_rank(user_name):
        response = requests.get(url=base_url+user_name)

        if response.ok:
            root = html.fromstring(response.content)
            profile_nav = root.xpath('//ul[@class="ProfileNav-list"]')[0]

            following = int(profile_nav.xpath('//a[@data-nav="following"]/span[@data-count]/@data-count')[0])
            followers = int(profile_nav.xpath('//a[@data-nav="followers"]/span[@data-count]/@data-count')[0])

            # normalized follower ratio from https://doi.org/10.1016/j.ipm.2016.04.003
            follower_rank = followers/(followers+following)

            print(f'follower_rank: {follower_rank}\n')
            return follower_rank
        else:
            response.raise_for_status()
