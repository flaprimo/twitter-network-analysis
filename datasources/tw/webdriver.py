from selenium import webdriver
from selenium.common.exceptions import TimeoutException
import chromedriver_binary
import logging
import copy

logger = logging.getLogger(__name__)


class WebDriver:
    def __init__(self, proxy_provider=None):
        self.chrome_options = WebDriver.__get_chrome_options()
        self.proxy_provider = proxy_provider

    @staticmethod
    def __get_chrome_options():
        url_blacklist = [
            'https://twitter.com/i/jot/*',
            '*.twitter.com/i/jot/*',
            'https://google-analytics.com/*',
            'https://www.google-analytics.com/*',
            'https://analytics.twitter.com/*'
        ]
        prefs = {
            'enable_do_not_track': True,
            'profile.default_content_setting_values.cookies': 2,
            'profile.managed_default_content_settings.images': 2,
            'disk-cache-size': 4096
        }
        arguments = [
            '--headless',
            '--no-sandbox',
            '--lang=en',
            '--blink-settings=imagesEnabled=false',
            '--disable-dev-shm-usage',
            '--window-position=0,0',
            '--window-size=1024,768',
            '--host-rules=' + ', '.join([f'MAP {url} localhost' for url in url_blacklist])
        ]

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option('prefs', prefs)
        for a in arguments:
            chrome_options.add_argument(a)

        return chrome_options

    def __get_driver(self):
        chrome_options = copy.deepcopy(self.chrome_options)

        if self.proxy_provider:
            logger.debug('getting proxy')
            proxy_ip, proxy_port, proxy_https, proxy_code = self.proxy_provider.get_proxy()
            chrome_options.add_argument(f'--proxy-server={proxy_ip}:{proxy_port}')

        logger.debug('getting driver')
        driver = webdriver.Chrome(executable_path=chromedriver_binary.chromedriver_filename,
                                  chrome_options=chrome_options)

        return driver

    def get_page(self, url, test_path, timeout=20):
        logger.info('loading page')
        driver = self.__get_driver()

        try:
            driver.set_page_load_timeout(timeout)
            driver.implicitly_wait(timeout)
            driver.get(url)

            # test if page loaded correctly
            if len(driver.find_elements_by_xpath(test_path)) > 0:
                logger.debug('page correctly loaded')
                return driver
            else:
                logger.debug('page not loaded correctly')
                driver.quit()
                return None

        except TimeoutException as e:
            logger.debug(f'url not loaded {str(e)}')
            driver.quit()
            return None
