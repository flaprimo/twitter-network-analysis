from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from datetime import datetime
from lxml import html
import chromedriver_binary
import logging
import time
import random
import re
import copy

logger = logging.getLogger(__name__)


class TwDynamicScraper:
    def __init__(self, base_url, proxy_provider):
        self.proxy_provider = proxy_provider
        self.base_url = base_url + 'search'

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
        self.chrome_options = chrome_options

    def __get_driver(self):
        chrome_options = copy.deepcopy(self.chrome_options)

        logger.debug('getting proxy')
        proxy_ip, proxy_port, proxy_https, proxy_code = self.proxy_provider.get_proxy()
        chrome_options.add_argument(f'--proxy-server={proxy_ip}:{proxy_port}')

        logger.debug('getting driver')
        driver = webdriver.Chrome(executable_path=chromedriver_binary.chromedriver_filename,
                                  chrome_options=chrome_options)
        driver.set_window_position(0, 0)
        driver.set_window_size(1024, 768)

        return driver

    @staticmethod
    def __load_tw_from_stream(driver, n):
        # load required number of tws
        tw_stream_len = {
            'before': 0,
            'after': len(driver.find_elements_by_xpath('//ol[@id="stream-items-id"]/'
                                                       'li[contains(@class, "stream-item")]'))
        }
        while n > tw_stream_len['after'] > tw_stream_len['before']:
            tw_stream_len['before'] = tw_stream_len['after']
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            tt_wait = random.uniform(3, 5)
            time.sleep(tt_wait)
            tw_stream_len['after'] = len(driver.find_elements_by_xpath('//ol[@id="stream-items-id"]/'
                                                                       'li[contains(@class, "stream-item")]'))
            logger.debug(f'time waited: {round(tt_wait, 2)}\n'
                         f'loading condition: tw to retrieve({n}) > '
                         f'tw retrieved after({tw_stream_len["after"]}) > '
                         f'tw retrieved before({tw_stream_len["before"]})')

        logger.debug('query results fetched')
        return driver

    @staticmethod
    def __get_tw(t):
        link = t.xpath('./div[@class="js-tweet-text-container"]/p')[0]
        tweet_footer = './div[@class="stream-item-footer"]/' \
                       'div[contains(@class, "ProfileTweet-actionCountList")]/' \
                       'span[contains(@class, "ProfileTweet-action--{0}")]/' \
                       'span/@data-tweet-stat-count'  # reply, retweet, favorite

        try:
            reply = t.xpath('./div[@class="ReplyingToContextBelowAuthor"]/a/@href').strip("/").lower()
        except:
            reply = []

        tw_current = {
            # header
            'author': t.xpath('./div[@class="stream-item-header"]/a/@href')[0].strip("/").lower(),
            'date': datetime.strptime(t.xpath('./div[@class="stream-item-header"]/small/a/@title')[0],
                                      '%I:%M %p - %d %b %Y'),
            # type
            'reply': reply,

            # content
            'language': t.xpath('./div[@class="js-tweet-text-container"]/p/@lang')[0],
            'text': t.xpath('./div[@class="js-tweet-text-container"]/p/text()')[0],
            'hashtags': ['#' + re.findall(r'/hashtag/(.+)\?', hashtag)[0]
                         for hashtag in link.xpath('./a[contains(@class, "twitter-hashtag")]/@href')],
            'emojis': [emoji for emoji in link.xpath('./img[contains(@class, "Emoji")]/@title')],
            'urls': [url for url in link.xpath('./a/@data-expanded-url')],
            'mentions': [reply.strip('/')
                         for reply in link.xpath('./a[contains(@class, "twitter-atreply")]/@href')],

            # footer
            'replies': t.xpath(tweet_footer.format('reply'))[0],
            'retweets': t.xpath(tweet_footer.format('retweet'))[0],
            'likes': t.xpath(tweet_footer.format('favorite'))[0]
        }

        return tw_current

    @staticmethod
    def __get_page(driver, url, test_path, timeout=20):
        logger.info('loading page')
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)

            # test if page loaded correctly
            if len(driver.find_elements_by_xpath(test_path)) > 0:
                logger.debug('page correctly loaded')
                return driver
            else:
                logger.debug('page not loaded correctly')
                driver.close()
                return None

        except TimeoutException as e:
            logger.debug(f'url not loaded {str(e)}')
            driver.close()
            return None

    def search(self, query, n=30):
        # get query url
        query_url = f'{self.base_url}?{query}&lang=en-gb'
        logger.info(f'getting search results for: {query_url}')

        # load queried web page
        driver = None
        while driver is None:
            driver = self.__get_page(self.__get_driver(), query_url,
                                     '//div[@class="SearchEmptyTimeline" or @class="stream"]')
        logger.debug('tw results page loaded')

        tw_list = []
        if len(driver.find_elements_by_xpath('//div[@class="SearchEmptyTimeline"]')) == 0:
            logger.debug('results are available')

            driver = TwDynamicScraper.__load_tw_from_stream(driver, n)

            # analyze loaded tws
            logger.debug('analyzing query results')
            tw_stream = driver.find_element_by_id('stream-items-id').get_attribute('innerHTML')
            driver.quit()
            tw_stream_xml = html.fromstring(tw_stream)

            for t in tw_stream_xml.xpath('./li[contains(@class, "stream-item")]/div/div[@class="content"]')[:n]:
                tw_current = TwDynamicScraper.__get_tw(t)
                tw_list.append(tw_current)
                logger.debug(f'added tw: {tw_current}')
        else:
            driver.quit()
            logger.debug('no results are available')

        logger.info(f'collected {len(tw_list)} tw')

        return tw_list
