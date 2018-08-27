class IO:
    # base dirs
    outputDir = 'output/'
    dataDir = 'data/'

    # input
    csvData = dataDir + 'll.csv'
    csvData_dtype = {
        'cod': str,
        'user_from_name': str,
        'user_from_fav_count': 'uint8',
        'user_rt_fav_count': 'uint8',
        'user_to_name': str,
        'text': str,
        'weights': 'uint8'
    }

    # output
    csvNodes_dtype = {
        'Id': str
    }
    csvEdges_dtype = {
        'Source': str,
        'Target': str,
        'Weight': 'uint8'
    }
    # output preprocessing
    csvEdges_PP = outputDir + 'preprocessing/edges.csv'
    # output community_detection
    communitiesOutput_CD = outputDir + 'community_detection/communities.txt'
    csvNodes_CD = outputDir + 'community_detection/nodes.csv'
    csvEdges_CD = outputDir + 'community_detection/edges.csv'
    # output analysis
    csvQualityMetrics_A = outputDir + 'analysis/quality_metrics.csv'
    csvNodes_A = outputDir + 'analysis/nodes.csv'


class DEMON:
    epsilon = 0.25
    min_community_size = 3
