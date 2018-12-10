import tweepy
import logging

logger = logging.getLogger(__name__)


class TwApi:
    def __init__(self): # consumer_key, consumer_key_secret, access_token, access_token_secret):

        logger.debug('INIT Tw api')
        self.consumerKey = "2QrWmgktSrXnFKlcTYCFn2GsF"
        self.consumerSecret = "826nBChHpDKVZDWfVNI8uQD3we2WPFK2xFaFZhihYY8n99fVU9"
        self.accessKey = "295370953-I7XuX1QonyQEJvEigrYnvxE2n4bR6p2quWyb8EPr"
        self.accessSecret = "zsgw6qkLR7G7JC8e9PA03sBVCzUKnPlKxcyV1ZEiyxNgA"
        self.api = None


    def create_search(self, query="", n=0):

        api = self.create_api()
        search = tweepy.Cursor(api.search, q=query).items(n)

        return search

    def create_stream(self):

        myStreamListener = MyStreamListener()
        return tweepy.Stream(auth = self.api.auth, listener=myStreamListener())



    def create_api(self):

        auth = tweepy.OAuthHandler(self.consumerKey, self.consumerSecret)
        auth.set_access_token(self.accessKey, self.accessSecret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        return self.api

    def get_api(self):

        return self.api