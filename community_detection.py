import networkx as nx
import pandas as pd
import demon as d
import re
import config


def main():
    print('# COMMUNITY DETECTION')

    g = load_graph()
    execute_demon(g)
    df_nodes = load_demon_communities()
    df_nodes = handle_lone_nodes(df_nodes)
    save_csv(df_nodes)


def load_graph():
    print('## LOAD GRAPH')

    df_edges = pd.read_csv(config.IO.csvEdges_PP, dtype=config.IO.csvEdges_dtype)
    g = nx.from_pandas_edgelist(df_edges,
                                source='Source', target='Target', edge_attr=['Weight'],
                                create_using=nx.DiGraph())

    print(f'  shape: {df_edges.shape}\n'
          f'  dataframe:\n{df_edges.head(5).to_string()}\n\n')

    return g


def execute_demon(g):
    print('## EXECUTE DEMON')

    dm = d.Demon(graph=g,
                 epsilon=config.DEMON.epsilon,
                 min_community_size=config.DEMON.min_community_size,
                 file_output=config.IO.communitiesOutput_CD)

    return dm.execute()


def load_demon_communities():
    print('## LOAD DEMON COMMUNITIES')

    communities = []
    with open(config.IO.communitiesOutput_CD, 'r') as f:
        for i, line in enumerate(f):
            c = line.split("\t", 1)[0]
            u = re.findall("'([^']*)'", line)
            for n in u:
                communities.append((c, n))
    print(f'  parsed community results (first 5): {communities[:5]}\n')

    # Results to dataframe
    df_communities = pd.DataFrame.from_records(communities, columns=['community', 'Id'], index='Id')
    print(f'  communities size:\n{df_communities.groupby("community").size().to_string()}\n')

    # Merge users from different communities together
    dummies = pd.get_dummies(df_communities['community'], prefix="C", dtype=bool)
    combine = pd.concat([df_communities, dummies], axis=1)
    df_communities = combine.groupby(df_communities.index).sum()

    print(f'  shape: {df_communities.shape}\n'
          f'  dataframe (first 5 rows):\n{df_communities.head(5).to_string()}\n\n')

    return df_communities


def handle_lone_nodes(df_nodes, action='remove'):
    def add_lone_nodes():
        print('## ADD LONE NODES')
        df_edges = pd.read_csv(config.IO.csvEdges_PP, dtype=config.IO.csvEdges_dtype)

        all_nodes = pd.concat([df_edges.Source, df_edges.Target]).drop_duplicates().to_frame('Id').set_index('Id')

        df_all_nodes = pd.concat([df_nodes, all_nodes], axis=1, sort=True).fillna(False)

        df_edges.to_csv(config.IO.csvEdges_CD, index=False)

        print(f'  lone nodes number: {all_nodes.shape[0] - df_nodes.shape[0]}\n'
              f'  shape: {df_nodes.shape}\n'
              f'  dataframe (first 5 rows):\n{df_nodes.head(5).to_string()}\n\n')

        return df_all_nodes

    def remove_lone_nodes_edges():
        print('## REMOVE LONE NODES')
        df_edges = pd.read_csv(config.IO.csvEdges_PP, dtype=config.IO.csvEdges_dtype)

        df_edges_together = df_edges[df_edges.Source.isin(df_nodes.index) & df_edges.Target.isin(df_nodes.index)]
        df_edges_together.to_csv(config.IO.csvEdges_CD, index=False)

        print(f'  shape before: {df_edges.shape}\n'
              f'  shape after: {df_edges_together.shape}\n'
              f'  lone edges number: {df_edges.shape[0] - df_edges_together.shape[0]}\n'
              f'  dataframe (first 5 rows):\n{df_edges.head(5).to_string()}\n\n')

        return df_nodes

    if action == 'add':
        return add_lone_nodes()
    else:
        return remove_lone_nodes_edges()


def save_csv(df_nodes):
    print('## SAVE CSV')

    df_nodes.to_csv(config.IO.csvNodes_CD)

    print(f'  shape: {df_nodes.shape}\n'
          f'  dataframe (first 5 rows):\n{df_nodes.head(5).to_string()}\n\n')


if __name__ == '__main__':
    main()
