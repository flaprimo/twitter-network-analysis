# Twitter network analysis
Directed network analysis, from a twitter csv data source to analysis and metric applications.

## Steps
Analysis is performed in several sequential steps, which produce intermediate results in the output.
1. preprocessing: read the original Twitter edges dataset and pre-process it (remove useless attributes, merge duplicates).
2. community_detection: apply the DEMON community detection algorithm to find overlapping communities. Lone nodes can either be removed (default) or added.
3. analysis: analyze the resulted communities from DEMON.

## Tools
* [Pandas](https://github.com/pandas-dev/pandas)
* [NetworkX](https://github.com/networkx)
* [Demon](https://github.com/GiulioRossetti/DEMON)