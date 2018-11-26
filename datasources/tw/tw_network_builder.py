import tweepy
import csv
import re
import networkx as nx
import pandas as pd


class NetworkBuilder():

    def __init__(self, file_path="graph_file", search = None):

        self.file_path = file_path
        self.from_name = []
        self.to_name = []
        self.tweets = []
        self.df = pd.DataFrame()
        self.df_result = None
        self.search = search

    def create_networks(self):

        for tweet in self.search: 
            if(tweet.text.startswith("RT")):
                mentions = re.findall(r'@\w+', tweet.text)
                for user in mentions:
                    user_screen_name = user.replace('@','').lower()
                    
                    self.tweets.append(tweet.text.replace('\n',''))
                    self.from_name.append(tweet.user.screen_name.lower())
                    self.to_name.append(user_screen_name)

                    self.tweets.append('NonNecessary')
                    self.from_name.append(user_screen_name)
                    self.to_name.append(tweet.user.screen_name)

     
    def save_csv(self):

        self.df['user_from_name'] = self.from_name
        self.df['user_to_name'] = self.to_name
        self.df['cod'] = self.df[['user_from_name', 'user_to_name']].apply(lambda x: ''.join(x), axis=1)
        self.df['text'] =  self.tweets

        self.df_result = self.df.groupby('cod').first()
        self.df_result['weights'] = self.df['cod'].value_counts()
        self.df_result.reset_index(inplace=True)
        
        self.df_result.to_csv("../database/csv_files/"+self.file_path,index = False,encoding='utf-8')    

    def create_graph(self):

        self.create_networks()
        self.save_csv()

        G = nx.Graph()
        G.add_nodes_from(self.df_result['user_from_name'])
        G.add_weighted_edges_from(zip(self.df_result['user_from_name'], 
                                      self.df_result['user_to_name'],
                                      self.df_result['weights']))

        G1 = nx.DiGraph()
        G1.add_nodes_from(self.df_result['user_from_name'])
        G1.add_edges_from(zip(self.df_result['user_from_name'],self.df_result['user_to_name']))

        return G, G1