import json
import os
import re
from datetime import datetime
import pytz
import requests_cache
from TwitterAPI import TwitterAPI, TwitterPager, TwitterError
import time
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class TwApi:
    def __init__(self, input_path, output_path):
        # set up http cache
        cache_path = os.path.join(output_path, 'tw_api')
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)
        self.cache_path = os.path.join(cache_path, 'cache')

        # read tw api config
        input_path = os.path.join(input_path, 'tw_api/tw_api.json')
        with open(input_path, 'r') as json_file:
            tw_api_account = json.load(json_file)
        self.api = TwitterAPI(
            tw_api_account['consumer_key'], tw_api_account['consumer_key_secret'],
            tw_api_account['access_token'], tw_api_account['access_token_secret'],
            auth_type='oAuth2')

        logger.debug('INIT Tw api')

    @staticmethod
    def parse_user(raw_user):
        def expand_url(url):
            import requests
            try:
                r = requests.get(url, timeout=5)
                return r.url if r.status_code != 200 else None
            except requests.exceptions.RequestException:
                return None

        return {
            'user_name': raw_user['screen_name'].lower(),
            'bio': re.sub(r'[\n\r\t]', ' ', raw_user['description']),
            'url': expand_url(raw_user['url']) if raw_user['url'] else None,
            'location': raw_user['location'],
            'followers': raw_user['followers_count'],
            'following': raw_user['friends_count'],
            'likes': raw_user['favourites_count'],
            'tweets': raw_user['statuses_count'],
            'language': raw_user['lang'],
            'join_date': datetime.strptime(raw_user['created_at'], '%a %b %d %H:%M:%S %z %Y').date(),
            'name': raw_user['name']
        }

    @staticmethod
    def parse_tweet(raw_tw):

        raw_tw_content = raw_tw['extended_tweet'] if 'extended_tweet' in raw_tw else raw_tw
        raw_tw_rt_content = \
            (raw_tw['retweeted_status']['extended_tweet']
             if 'extended_tweet' in raw_tw['retweeted_status'] else raw_tw['retweeted_status']) \
            if 'retweeted_status' in raw_tw else None

        tw = {
            'tw_id': raw_tw['id'],
            'user_name': raw_tw['user']['screen_name'].lower(),
            'date': datetime.strptime(raw_tw['created_at'], '%a %b %d %H:%M:%S %z %Y')
            .astimezone(pytz.UTC).replace(tzinfo=None),
            'lang': raw_tw['lang'],
            'no_likes': raw_tw['favorite_count'],
            'no_retweets': raw_tw['retweet_count'],
            'is_retweet': 'retweeted_status' in raw_tw,
            'reply': raw_tw['in_reply_to_screen_name'].lower() if raw_tw['in_reply_to_screen_name'] else None,

            # text
            'text': raw_tw_content['full_text'] if 'extended_tweet' in raw_tw else raw_tw_content['text'],

            # entities
            'is_media': 'retweeted_status' in raw_tw_content['entities'],
            'hashtags': ['#' + h['text'].lower() for h in raw_tw_content['entities']['hashtags']],
            'mentions': [m['screen_name'].lower() for m in raw_tw_content['entities']['user_mentions']],
            'urls': [u['expanded_url'] for u in raw_tw_content['entities']['urls']],

            # retweet
            'retweeted_hashtags': ['#' + h['text'].lower() for h in raw_tw_rt_content['entities']['hashtags']]
            if 'retweeted_status' in raw_tw else []
        }

        patterns = [
            r'^RT @\w+: ',
            r'https?:\/\/t.co\/\w+',
            r'(@|#)\w+',
            r'(\w+| *)…$'
        ]

        tw['text'] = re.sub('|'.join(f'({p})' for p in patterns), '', tw['text'])
        tw['text'] = re.sub(r'(\n|\t| {2})+', ' ', tw['text'])
        tw['text'] = tw['text'].strip()

        # tw_record['text'] = re.sub(r'^RT @\w+: ', '', tw_record['text'])
        # tw_record['text'] = re.sub(r'https?:\/\/t.co\/\w+', '', tw_record['text'])
        # tw_record['text'] = re.sub(r'(@|#)\w*', '', tw_record['text'])
        # tw_record['text'] = re.sub(r'\n|\t|  +', ' ', tw_record['text'])
        # tw_record['text'] = re.sub(r'(\w+…|…)$', '', tw_record['text'])
        # tw_record['text'] = re.sub(r'  +', '', tw_record['text'])
        # tw_record['text'] = tw_record['text'].strip()

        return tw

    def __get_tweets(self, pager, n, from_date=None, to_date=None, wait=5, parse=True):
        tw_list = []

        with requests_cache.enabled(self.cache_path, expire_after=86400):
            try:
                for i, raw_tw in zip(range(n), pager.get_iterator(wait=wait)):
                    if 'message' in raw_tw:
                        logger.debug(f'{raw_tw["message"]} ({raw_tw["code"]})')
                    else:
                        tw = self.parse_tweet(raw_tw)
                        date = tw['date'].date()

                        if from_date and from_date > date:
                            return tw_list
                        elif not (to_date and to_date < date):
                            tw_list.append(tw if parse else raw_tw)

            except TwitterError.TwitterError:
                pass

        return tw_list

    def premium_search(self, product='fullarchive', label='prod', query='', since=None, until=None, n=100):
        logger.info(f'tw api search for: {query}')

        pager = TwitterPager(self.api, f'tweets/search/{product}/:{label}',
                             {'query': query,
                              'fromDate': since.strftime('%Y%m%d%H%M'),
                              'toDate': until.strftime('%Y%m%d%H%M')})

        return self.__get_tweets(pager, n, parse=False)

    # limits are 200 per page, top 3200. app auth is 1500 req/15min.
    def get_user_timeline(self, user_name, n=200, from_date=None, to_date=None):
        logger.info(f'tw api timeline for user: {user_name}')

        pager = TwitterPager(self.api, 'statuses/user_timeline',
                             {'screen_name': user_name,
                              'count': n,
                              'exclude_replies': 'true'})

        return self.__get_tweets(pager, n, from_date, to_date, 2)

    def get_user_timelines(self, user_name_list, n, from_date=None, to_date=None):
        logger.info(f'tw api timeline for {len(user_name_list)} users')
        wait = 2

        stream = []
        for u in tqdm(user_name_list):
            start_time = time.time()
            user_stream = self.get_user_timeline(u, n, from_date, to_date)
            stream.extend(user_stream)

            elapsed = time.time() - start_time
            pause = wait - elapsed
            if pause > 0:
                time.sleep(pause)

        return stream

    def get_user_profiles(self, user_name_list):
        logger.info(f'tw api profiles for {len(user_name_list)} users')
        # group usernames in 100 lists
        u_groups = [user_name_list[n:n + 100] for n in range(0, len(user_name_list), 100)]
        wait = 3

        stream = []
        for u_list in u_groups:
            start_time = time.time()
            with requests_cache.enabled(self.cache_path, expire_after=86400):
                u_stream = self.api.request('users/lookup', {'screen_name': u_list, 'include_entities': 'false'})

            for u in u_stream:
                stream.append(self.parse_user(u))

            elapsed = time.time() - start_time
            pause = wait - elapsed
            if pause > 0:
                time.sleep(pause)

        return stream

    def get_rate_limit_status(self, resources):
        rate_limit_status = self.api.request('application/rate_limit_status', {'resources': resources}).json()

        return rate_limit_status
