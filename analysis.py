import networkx as nx
import pandas as pd
import config


def main():
    g = load_graph()
    c_subgraphs = get_community_subgraphs(g)

    # print('# H-INDEX'
    #       f'(top 10 per community)')
    # for c_label, c in c_subgraphs:
    #     print(f'community {c_label}: {compute_h_index(c)[:10]}')
    print(compute_h_index(c_subgraphs[3][1])[:10])


def load_graph():
    print('# LOAD GRAPH')

    df_nodes = pd.read_csv(config.IO.csvNodes_CD, index_col='Id', dtype=config.IO.csvNodes_dtype)
    df_edges = pd.read_csv(config.IO.csvEdges_CD, dtype=config.IO.csvEdges_dtype)

    # remove nodes not belonging to a community
    print(df_edges.shape)
    df_edges = df_edges[df_edges.Source.isin(df_nodes.index) & df_edges.Target.isin(df_nodes.index)]
    print(df_edges.shape)

    print('## Load dataframes\n'
          f'NODES\n  shape: {df_nodes.shape}\n  dataframe:\n{df_nodes.head(5).to_string()}\n\n'
          f'EDGES\n  shape: {df_edges.shape}\n  dataframe:\n{df_edges.head(5).to_string()}\n\n')

    g = nx.from_pandas_edgelist(df_edges,
                                source='Source', target='Target', edge_attr=['Weight'],
                                create_using=nx.DiGraph())

    for c in df_nodes.columns:
        nx.set_node_attributes(g, pd.Series(df_nodes[c]).to_dict(), c)

    print(f'## Load graph\n'
          f'  number of nodes: {len(g)}\n  nodes (first 3):\n {list(g.nodes(data=True))[:3]}\n\n')

    return g


def get_community_labels(g):
    return list(list(g.nodes(data=True))[:1][0][1].keys())


def get_community_subgraphs(g):
    communities = get_community_labels(g)
    c_subgraphs = []

    for c in communities:
        c_nodes = [x for x, y in g.nodes(data=True) if y[c]]
        c_subgraph = nx.DiGraph(g.subgraph(c_nodes))

        # remove node attributes to keep memory low
        for n in c_subgraph.nodes(data=True):
            for com in communities:
                n[1].pop(com, None)

        c_subgraphs.append((c, c_subgraph))

    return c_subgraphs


def compute_h_index(g):
    g_hindex = []

    for n in g.nodes:
        print(n)

    # for n in g.nodes:
    #     edges = [e[2]['Weight'] for e in g.in_edges(n, data=True)]
    #     g_hindex.append((n, h_index(edges)))

    return sorted(g_hindex, key=lambda x: x[1])


#  from https://github.com/kamyu104/LeetCode/blob/master/Python/h-index.py
def h_index(citations):
    """
    :type citations: List[int]
    :rtype: int
    """
    citations.sort(reverse=True)
    h = 0
    for x in citations:
        if x >= h + 1:
            h += 1
        else:
            break
    return h
