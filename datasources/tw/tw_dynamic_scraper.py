from datetime import datetime
from lxml import html
import logging
import time
import random
import re
from selenium.common.exceptions import TimeoutException

from .webdriver import WebDriver

logger = logging.getLogger(__name__)


class TwDynamicScraper:
    def __init__(self, base_url, proxy_provider):
        self.webdriver = WebDriver(proxy_provider)
        self.base_url = base_url + 'search'

    @staticmethod
    def __load_tw_from_stream(driver, n):
        tw_stream_len_after = None
        while tw_stream_len_after is None:
            try:
                tw_stream_len_after = len(driver.find_elements_by_xpath('//ol[@id="stream-items-id"]/'
                                                                        'li[contains(@class, "stream-item")]'))
            except TimeoutException:
                logger.debug('timeout exception in fetching stream length, retrying')
                tw_stream_len_after = None

        # load required number of tws
        tw_stream_len = {
            'before': 0,
            'after': tw_stream_len_after
        }
        while n > tw_stream_len['after'] > tw_stream_len['before']:
            tw_stream_len['before'] = tw_stream_len['after']

            has_scrolled = False
            tt_wait = 0
            while not has_scrolled:
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                    has_scrolled = True
                except TimeoutException:
                    tt_wait += 1
                    time.sleep(tt_wait)
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
        tw_header = t.xpath('./div[@class="stream-item-header"]')[0]
        tw_content = t.xpath('./div[@class="js-tweet-text-container"]/p')[0]
        tw_footer = t.xpath('./div[@class="stream-item-footer"]/'
                            'div[contains(@class, "ProfileTweet-actionCountList")]')[0]
        # reply, retweet, favorite
        tw_action = './span[contains(@class, "ProfileTweet-action--{0}")]/span/@data-tweet-stat-count'

        tw_current = {
            # header
            'author': tw_header.xpath('./a/@href')[0].strip("/").lower(),
            'date': datetime.strptime(tw_header.xpath('./small/a/@title')[0], '%I:%M %p - %d %b %Y'),
            'tw_id': tw_header.xpath('./small/a/@data-conversation-id')[0],

            # type
            'reply': [reply.strip('/').lower()
                      for reply in t.xpath('./div[@class="ReplyingToContextBelowAuthor"]/a/@href')],

            # content
            'language': tw_content.xpath('./@lang')[0],
            'text': next(iter(tw_content.xpath('./text()')), ''),
            'hashtags': ['#' + re.findall(r'/hashtag/(.+)\?', hashtag)[0].lower()
                         for hashtag in tw_content.xpath('./a[contains(@class, "twitter-hashtag")]/@href')],
            'emojis': tw_content.xpath('./img[contains(@class, "Emoji")]/@title'),
            'urls': tw_content.xpath('./a/@data-expanded-url'),
            'mentions': [mention.strip('/').lower()
                         for mention in tw_content.xpath('./a[contains(@class, "twitter-atreply")]/@href')],

            # footer
            'no_replies': int(tw_footer.xpath(tw_action.format('reply'))[0]),
            'no_retweets': int(tw_footer.xpath(tw_action.format('retweet'))[0]),
            'no_likes': int(tw_footer.xpath(tw_action.format('favorite'))[0])
        }

        return tw_current

    def search(self, query, n=30):
        # get query url
        query_url = f'{self.base_url}?{query}&lang=en-gb'
        logger.info(f'getting search results for: {query_url}')

        # load queried web page
        driver = None
        while driver is None:
            driver = self.webdriver.get_page(query_url, '//div[@class="SearchEmptyTimeline" or @class="stream"]')
        logger.debug('tw results page loaded')

        tw_list = []
        if len(driver.find_elements_by_xpath('//div[@class="SearchEmptyTimeline"]')) == 0:
            logger.debug('results are available')

            driver = TwDynamicScraper.__load_tw_from_stream(driver, n)

            # analyze loaded tws
            logger.debug('analyzing query results')
            tw_stream = None
            while tw_stream is None:
                try:
                    tw_stream = driver.find_element_by_id('stream-items-id').get_attribute('innerHTML')
                except TimeoutException:
                    logger.debug('timeout exception in fetching stream, retrying')
                    tw_stream = None
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
