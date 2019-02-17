from datetime import datetime
from lxml import html
import logging
import time
import random
import re
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from .webdriver import WebDriver

logger = logging.getLogger(__name__)


class TwDynamicScraper:
    def __init__(self, base_url, proxy_provider):
        self.webdriver = WebDriver(proxy_provider)
        self.base_url = base_url + 'search?{0}&lang=en-gb'

    @staticmethod
    def __get_tw_stream(driver, n):
        stream_len_before = 0
        stream_len_after =\
            len(driver.find_elements_by_xpath('//ol[@id="stream-items-id"]/li[contains(@class, "stream-item")]'))

        while n > stream_len_after > stream_len_before:
            stream_len_before = stream_len_after

            has_scrolled = False
            tt_wait = 0
            while not has_scrolled:
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                    has_scrolled = True
                except TimeoutException:
                    tt_wait += 1
                    time.sleep(tt_wait)

            time.sleep(random.uniform(3, 5))
            stream_len_after =\
                len(driver.find_elements_by_xpath('//ol[@id="stream-items-id"]/li[contains(@class, "stream-item")]'))

            logger.debug(f'time waited: {round(tt_wait, 2)}\n'
                         f'loading condition: tw to retrieve({n}) > '
                         f'tw retrieved after({stream_len_after}) > '
                         f'tw retrieved before({stream_len_before})')

        logger.debug('query results fetched')
        return driver

    @staticmethod
    def __get_tw_list(tw_stream_xml, n):
        tw_list = []
        for t in tw_stream_xml.xpath('./li[contains(@class, "stream-item")]/div/div[@class="content"]')[:n]:
            tw_current = TwDynamicScraper.__get_tw(t)
            tw_list.append(tw_current)
            logger.debug(f'added tw: {tw_current}')

        logger.info(f'collected {len(tw_list)} tw')

        return tw_list

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
        query_url = self.base_url.format(query)
        logger.info(f'getting search results for: {query_url}')

        is_stream_loaded = False
        tw_stream_xml = None

        while not is_stream_loaded:
            driver = self.webdriver.get_page(query_url, '//div[@class="SearchEmptyTimeline" or @class="stream"]')

            try:
                # load queried web page
                logger.debug('tw results page loaded')

                # load tw stream
                driver = TwDynamicScraper.__get_tw_stream(driver, n)
                tw_stream = driver.find_element_by_id('stream-items-id').get_attribute('innerHTML')
                tw_stream_xml = html.fromstring(tw_stream)
                is_stream_loaded = True

            except NoSuchElementException as e:
                logger.debug(f'timeline is empty: {str(e)}')
                is_stream_loaded = True

            except TimeoutException as e:
                logger.debug(f'parsing tw failed, retrying: {str(e)}')

            finally:
                driver.quit()

        if tw_stream_xml:
            return TwDynamicScraper.__get_tw_list(tw_stream_xml, n)
        else:
            return []
