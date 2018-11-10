# Twitter network analysis
<a href='https://travis-ci.org/flaprimo/twitter-network-analysis'><img src='https://secure.travis-ci.org/flaprimo/twitter-network-analysis.png?branch=infomap'></a>

Directed network analysis, from Twitter data source to analysis and metric applications.

## Steps
Analysis is performed in several sequential/parallel pipelines, which produce intermediate results in the output.
Steps are WIP stay tuned ;)

## Installation
1. install python required modules `pip install requirements.txt`

2. install chrome driver for [Selenium](https://selenium-python.readthedocs.io/installation.html#drivers)

   `wget https://chromedriver.storage.googleapis.com/2.43/chromedriver_linux64.zip`
   
   `sudo unzip chromedriver_linux64.zip -d /usr/local/bin`
   
   notes:
      * latest version of chrome driver at the moment of writing is 2.43
      * chrome driver must be placed in a folder in PATH