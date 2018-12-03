from datetime import datetime, timedelta
from random import shuffle
from selenium.webdriver.support.select import Select
from lxml import html
import logging
import socket
import json
from .webdriver import WebDriver

logger = logging.getLogger(__name__)


class ProxyProvider:
    def __init__(self, provider, proxy_list_path, expires=timedelta(days=1)):
        self.provider = provider
        self.webdriver = WebDriver()
        self.expires = expires
        self.proxy_list_path = proxy_list_path

        self.proxy_list = self.__get_proxy_list()
        self.proxy_list_len = len(self.proxy_list['list'])
        self.index = 0

    def get_proxy(self):
        proxy = self.proxy_list['list'][self.index]
        self.index = (self.index + 1) % self.proxy_list_len

        return proxy['ip'], proxy['port'], proxy['https'], proxy['code']

    @staticmethod
    def __is_server_alive(ip, port, timeout=1):
        socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_client.settimeout(timeout)
        try:
            return socket_client.connect_ex((ip, int(port))) == 0
        except socket.timeout:
            return False

    def __get_proxy_list(self):
        logger.info('getting proxy list')
        try:
            proxy_list = self.__read_proxy_list()
            if datetime.now() - proxy_list['date'] > self.expires:
                logger.debug('proxy list expired')
                # download new list and write it on file
                proxy_list = self.__fetch_proxy_list()
        except (OSError, IOError):
            logger.debug('proxy list not present')
            proxy_list = self.__fetch_proxy_list()

        shuffle(proxy_list['list'])

        return proxy_list

    def __fetch_proxy_list(self):
        logger.info('fetching new proxy list')

        # load proxy list web page
        driver = None
        while driver is None:
            driver = self.webdriver.get_page(self.provider, '//table[@id="proxylisttable"]')
        logger.debug('proxy list table page loaded')

        # change option value to max length
        table_length_option = driver.find_element_by_xpath('//select[@name="proxylisttable_length"]/option[@value=80]')
        driver.execute_script('arguments[0].value = arguments[1]', table_length_option, '300')

        # select option
        table_length = Select(driver.find_element_by_name('proxylisttable_length'))
        table_length.select_by_value('300')

        # get the proxy table
        proxy_list = {
            'date': datetime.now(),
            'list': []
        }
        table = driver.find_elements_by_xpath('//table[@id="proxylisttable"]/tbody')[0].get_attribute('innerHTML')
        driver.quit()

        # get proxies
        table = html.fromstring(table)
        for row in table.xpath('.//tr'):
            cells = row.xpath('.//td/text()')
            proxy_list['list'].append({
                'ip': cells[0],
                'port': cells[1],
                'code': cells[2],
                'anonymity': cells[4],
                'https': cells[6] == 'yes'
            })

        # filter out low privacy proxies
        logger.debug('filter low privacy proxies')
        proxy_list['list'] = list(filter(lambda p: p['anonymity'] != 'transparent',
                                         proxy_list['list']))

        # filter out slow proxies
        logger.debug('filter slow proxies')
        proxy_list['list'] = list(filter(lambda p: ProxyProvider.__is_server_alive(p['ip'], p['port']),
                                         proxy_list['list']))

        logger.debug(f'fetched {len(proxy_list["list"])} proxies at {proxy_list["date"]}')

        self.__save_proxy_list(proxy_list, self.proxy_list_path)

        return proxy_list

    def __read_proxy_list(self):
        logger.info('reading proxy list json file')
        with open(self.proxy_list_path) as json_file:
            proxy_list = json.load(json_file)
            proxy_list['date'] = datetime.strptime(proxy_list['date'], "%Y-%m-%dT%H:%M:%S.%f")

        return proxy_list

    @staticmethod
    def __save_proxy_list(proxy_list, proxy_list_path):
        logger.info('saving proxy list json file')

        # save proxy list usages
        proxy_list['date'] = proxy_list['date'].isoformat()

        # write proxy list json file
        with open(proxy_list_path, 'w') as json_file:
            json.dump(proxy_list, json_file, indent=4)
