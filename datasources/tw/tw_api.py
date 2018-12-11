import tweepy
import logging

logger = logging.getLogger(__name__)


class TwApi:
    def __init__(self, consumer_key, consumer_key_secret, access_token, access_token_secret):
        self.consumerKey = consumer_key
        self.consumerSecret = consumer_key_secret
        self.accessKey = access_token
        self.accessSecret = access_token_secret

        auth = tweepy.OAuthHandler(self.consumerKey, self.consumerSecret)
        auth.set_access_token(self.accessKey, self.accessSecret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        logger.debug('INIT Tw api')

    def create_search(self, query='', n=0):
        logger.info(f'tw api search for: {query}')
        search = tweepy.Cursor(self.api.search, q=query, tweet_mode='extended').items(n)

        return search

    # def create_stream(self):
    #     myStreamListener = MyStreamListener()
    #
    #     return tweepy.Stream(auth=self.api.auth, listener=myStreamListener())
