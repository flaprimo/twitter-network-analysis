from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import random
import re
from datetime import datetime
from lxml import html
import logging

logger = logging.getLogger(__name__)


class TwDynamicScraper:
    def __init__(self, base_url, proxy_provider):
        self.proxy_provider = proxy_provider
        self.base_url = base_url + 'search'

    def search(self, query, n_scroll=0):
        logger.info('getting search results')
        # get query url
        query_url = f'{self.base_url}?{query}'

        # get proxy
        logger.debug('getting proxy')
        proxy_ip, proxy_port, proxy_https, proxy_code = self.proxy_provider.get_proxy()

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
        chrome_options.add_argument('--lang=en')
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument(f'--proxy-server={proxy_ip}:{proxy_port}')

        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.set_window_position(0, 0)
        driver.set_window_size(1024, 768)

        # load queried web page
        driver.get(query_url)

        # stream = driver.find_element_by_id('streams-items-id').get_attribute('innerHTML')
        # table = driver.find_elements_by_xpath('//ol[@id="streams-items-id"]/li')

        # print(stream)

        # for _ in range(n_scroll):
        #     driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        #     WebDriverWait(driver, random.uniform(1, 3))

        logger.debug('query results fetched')
        logger.debug('analyzing query results')
        twitter_stream = driver.find_element_by_id('stream-items-id')

        tweets = []
        current_stream = twitter_stream.get_attribute('innerHTML')
        current_stream_xml = html.fromstring(current_stream)
        for t in current_stream_xml.xpath('./li/div/div[@class="content"]'):
            tweet = {}

            # header
            tweet['author'] = t.xpath('./div[@class="stream-item-header"]/a/@href')[0].strip("/")  # /runmanantiale
            date = t.xpath('./div[@class="stream-item-header"]/small/a/@title')[0] # 5:52 am - 12 Nov. 2018
            try:
                tweet['date'] = datetime.strptime(date, "%I:%M %p - %d %b %Y")
            except ValueError:
                tweet['date'] = datetime.strptime(date, "%I:%M %p - %d %b. %Y")

            # content
            tweet['language'] = t.xpath('./div[@class="js-tweet-text-container"]/p/@lang')[0]  # es
            tweet['text'] = t.xpath('./div[@class="js-tweet-text-container"]/p/text()')[0]

            # need better loop!
            for link in t.xpath('./div[@class="js-tweet-text-container"]/p'):
                tweet['hashtags'] = []
                for hashtag in link.xpath('./a[contains(@class, "twitter-hashtag")]/@href'):
                    tweet['hashtags'].append('#' + re.findall('/hashtag/(.+)\?', hashtag)[0])

                tweet['emojis'] = []
                for emoji in link.xpath('./img[contains(@class, "Emoji")]/@title'):
                    tweet['emojis'].append(emoji)

                tweet['urls'] = []
                for url in link.xpath('./a/@data-expanded-url'):
                    tweet['urls'].append(url)

                tweet['reply'] = []
                for reply in link.xpath('./a[contains(@class, "twitter-atreply")]/@href'):
                    tweet['reply'].append(reply.strip('/'))

            # footer
            tweet_footer = './div[@class="stream-item-footer"]/div[contains(@class, "ProfileTweet-actionCountList")]/' \
                           'span[contains(@class, "ProfileTweet-action--{0}")]/span/@data-tweet-stat-count'  # reply, retweet, favorite
            tweet['replies'] = t.xpath(tweet_footer.format('reply'))[0]
            tweet['retweets'] = t.xpath(tweet_footer.format('retweet'))[0]
            tweet['likes'] = t.xpath(tweet_footer.format('favorite'))[0]

            print(tweet)


        driver.quit()

        return {}
