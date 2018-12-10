import tweepy
import pandas as pd
from tw_network_builder import *
from datasources.tw.tw import tw

if __name__ == "__main__":
    # FILTERING TWEETS
    search = tw.tw_api.create_search(query="#flamengo", n=100)

    # CREATIN NETWORKS BUILDER
    G = NetworkBuilder(file_path="#flamengo_networks.csv", search=search)

    # RETURNING A GRAPH AND A DIGRAPH
    graph, digraph = G.create_graph()