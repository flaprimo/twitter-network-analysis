from datetime import datetime, timedelta
from random import shuffle
from selenium import webdriver
from selenium.webdriver.support.select import Select
from lxml import html
import socket
import json


class ProxyProvider:
    def __init__(self, provider, proxy_list_path, expires=timedelta(days=1)):
        self.provider = provider
        self.expires = expires
        self.proxy_list_path = proxy_list_path

        self.proxy_list = self.__get_proxy_list()
        self.index = 0

        self.__save_proxy_list()

    def get_proxy(self):
        # reset index list
        if self.index >= len(self.proxy_list['list']):
            self.index = 0

        proxy = self.proxy_list['list'][self.index]
        self.index += 1

        if self.__is_server_alive(proxy['ip'], proxy['port']):
            proxy['usage_count'] += 1
            self.__save_proxy_list()
            return proxy['ip'], proxy['port'], proxy['https']
        else:
            return self.get_proxy()

    @staticmethod
    def __is_server_alive(ip, port):
        socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_client.settimeout(1)
        try:
            return socket_client.connect_ex((ip, int(port))) == 0
        except socket.timeout:
            return False

    def __get_proxy_list(self):
        try:
            proxy_list = self.__read_proxy_list()
            if datetime.now() - proxy_list['date'] > self.expires:
                # download new list and write it on file
                proxy_list = self.__fetch_proxy_list()
        except (OSError, IOError):
            proxy_list = self.__fetch_proxy_list()
        shuffle(proxy_list['list'])
        sorted(proxy_list['list'], key=lambda p: p['usage_count'])

        return proxy_list

    def __fetch_proxy_list(self):
        # set selenium options
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            'profile.default_content_setting_values.cookies': 2,
            'profile.managed_default_content_settings.images': 2,
            'disk-cache-size': 4096
        }
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_argument('headless')

        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.set_window_position(0, 0)
        driver.set_window_size(1024, 768)

        # load proxy list web page
        driver.get(self.provider)

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
                'https': cells[6] == 'yes',
                'usage_count': 0
            })

        # filter out low privacy proxies
        proxy_list['list'] = [p for p in proxy_list['list'] if p['anonymity'] != 'transparent']

        return proxy_list

    def __read_proxy_list(self):
        with open(self.proxy_list_path) as json_file:
            proxy_list = json.load(json_file)
            proxy_list['date'] = datetime.strptime(proxy_list['date'], "%Y-%m-%dT%H:%M:%S.%f")

        return proxy_list

    def __save_proxy_list(self):
        # save proxy list usages
        proxy_list = self.proxy_list.copy()
        proxy_list['date'] = proxy_list['date'].isoformat()

        # write proxy list json file
        with open(self.proxy_list_path, 'w') as json_file:
            json.dump(proxy_list, json_file, indent=4)
