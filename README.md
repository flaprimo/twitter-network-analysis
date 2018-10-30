# Twitter network analysis
<a href='https://travis-ci.org/flaprimo/UniversityBot'><img src='https://secure.travis-ci.org/flaprimo/twitter-network-analysis.png?branch=master'></a>
Directed network analysis, from a twitter csv data source to analysis and metric applications.

## Steps
Analysis is performed in several sequential steps, which produce intermediate results in the output.
1. preprocessing: read the original Twitter edges dataset and pre-process it (remove useless attributes, merge duplicates).
2. community_detection: apply the DEMON community detection algorithm to find overlapping communities. Lone nodes can either be removed (default) or added.
3. metrics: computed metrics against the resulted communities from DEMON.

Analysis of the obtained results is then performed, against different epsilon settings of the DEMON algorithm, different network datasets and against other algorithms that find modularity.