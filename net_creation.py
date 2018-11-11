import csv
import time
import sys
import json
import re
import pandas as pd

df = pd.DataFrame()

sn = []
ids = []
from_name = []
to_name = []
from_fv_count = []
to_fv_count = []
tweets = []

with open('data/#KDD.txt', 'r') as f:
    search = [json.loads(l) for l in f]

print(search)

for tweet in search:
    if tweet.text.startswith("RT"):
        mentions = re.findall(r'@\w+', tweet.text)
        for user in mentions:
            user_screen_name = user.replace('@', '')
            tweets.append(tweet.text.replace('\n', ''))
            from_name.append(tweet.user.screen_name)
            to_name.append(user_screen_name)
            from_fv_count.append(10)
            to_fv_count.append(10)

            from_name.append(user_screen_name)
            tweets.append('NonNecessary')
            to_name.append(tweet.user.screen_name)
            from_fv_count.append(10)
            to_fv_count.append(10)

df['user_from_name'] = from_name
df['user_from_fav_count'] = from_fv_count
df['user_rt_fav_count'] = to_fv_count
df['user_to_name'] = to_name
df['cod'] = df[['user_from_name', 'user_to_name']].apply(lambda x: ''.join(x), axis=1)
df['text'] = tweets

df_result = df.groupby('cod').first()
df_result['weights'] = df['cod'].value_counts()
df_result.reset_index(inplace=True)

df_result.to_csv('data/output_json_kdd', index=False, encoding='utf-8')

print("Tweets Saved")
