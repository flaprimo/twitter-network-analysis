import tweepy
import logging

logger = logging.getLogger(__name__)


class TwApi:
    def __init__(self, consumer_key, consumer_key_secret, access_token, access_token_secret):
        auth = tweepy.OAuthHandler(consumer_key, consumer_key_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        logger.debug('INIT Tw api')

        # consumer_key = "2QrWmgktSrXnFKlcTYCFn2GsF"
        # consumer_key_secret = "826nBChHpDKVZDWfVNI8uQD3we2WPFK2xFaFZhihYY8n99fVU9"
        # access_token = "295370953-I7XuX1QonyQEJvEigrYnvxE2n4bR6p2quWyb8EPr"
        # access_token_secret = "zsgw6qkLR7G7JC8e9PA03sBVCzUKnPlKxcyV1ZEiyxNgA"

    def create_search(self, query="", n=100):
        return tweepy.Cursor(self.api.search, q=query).items(n)
