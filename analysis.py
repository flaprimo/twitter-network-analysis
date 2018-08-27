import networkx as nx
import pandas as pd
from tqdm import tqdm
import config


def main():
    g = load_graph()
    c_subgraphs = get_community_subgraphs(g)

    c_hindex_list = get_hindex(c_subgraphs)
    c_indegree_list = get_indegree(c_subgraphs)


def load_graph():
    print('# LOAD GRAPH')

    df_nodes = pd.read_csv(config.IO.csvNodes_CD, index_col='Id', dtype=config.IO.csvNodes_dtype)
    df_edges = pd.read_csv(config.IO.csvEdges_CD, dtype=config.IO.csvEdges_dtype)

    print('## Load dataframes\n'
          f'NODES\n'
          f'  shape: {df_nodes.shape}\n'
          f'  dataframe (first 5 rows):\n{df_nodes.head(5).to_string()}\n'
          f'EDGES\n'
          f'  shape: {df_edges.shape}\n'
          f'  dataframe (first 5 rows):\n{df_edges.head(5).to_string()}\n\n')

    g = nx.from_pandas_edgelist(df_edges,
                                source='Source', target='Target', edge_attr=['Weight'],
                                create_using=nx.DiGraph())

    for c in df_nodes.columns:
        nx.set_node_attributes(g, pd.Series(df_nodes[c]).to_dict(), c)

    print(f'## Load graph\n'
          f'  number of nodes: {len(g)}\n'
          f'  number of edges: {g.number_of_edges()}\n'
          f'  nodes (first 2):\n {list(g.nodes(data=True))[:2]}\n\n')

    return g


def get_community_labels(g):
    return list(list(g.nodes(data=True))[:1][0][1].keys())


def get_community_subgraphs(g):
    print('# GET COMMUNITY SUBGRAPHS')

    communities = get_community_labels(g)
    c_subgraphs = []

    for c in tqdm(communities):
        c_nodes = [x for x, y in g.nodes(data=True) if y[c]]
        c_subgraph = nx.DiGraph(g.subgraph(c_nodes))

        # remove node attributes to keep memory low
        for n in c_subgraph.nodes(data=True):
            for com in communities:
                n[1].pop(com, None)

        c_subgraphs.append((c, c_subgraph))

    print(f'  number of communities: {len(communities)}\n'
          f'  community list: {communities}\n'
          f'  communities (only first node for each community is shown):'
          f'{[(c[0], list(c[1].nodes(data=True))[1]) for c in c_subgraphs]}\n\n')

    return c_subgraphs


def get_hindex(c_subgraphs):
    print('# H-INDEX')

    def compute_subgraph_hindex(g):
        for n in g.nodes:
            edges = [e[2]['Weight'] for e in g.in_edges(n, data=True)]
            g.node[n]['hindex'] = alg_hindex(edges)

    # from https://github.com/kamyu104/LeetCode/blob/master/Python/h-index.py
    def alg_hindex(citations):
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

    c_hindex_list = []
    for c_label, c in c_subgraphs:
        compute_subgraph_hindex(c)
        c_hindex = nx.get_node_attributes(c, 'hindex')
        c_hindex = sorted(c_hindex.items(), key=lambda x: x[1], reverse=True)
        c_hindex_list.append((c_label, c_hindex))

    print(f'  h index (show first 10 nodes per community):')
    for c_label, c in c_hindex_list:
        print(f'  {c_label}: {c[:10]}')
    print('\n')

    return c_hindex_list


def get_indegree(c_subgraphs):
    print('# IN-DEGREE')

    def compute_subgraph_indegree(g):
        for n in g.nodes:
            g.node[n]['indegree'] = g.in_degree(n)

    c_indegree_list = []
    for c_label, c in c_subgraphs:
        compute_subgraph_indegree(c)
        c_indegree = nx.get_node_attributes(c, 'indegree')
        c_indegree = sorted(c_indegree.items(), key=lambda x: x[1], reverse=True)
        c_indegree_list.append((c_label, c_indegree))

    print('  indegree (show first 10 nodes per community):')
    for c_label, c in c_indegree_list:
        print(f'  {c_label}: {c[:10]}')
    print('\n')

    return c_indegree_list


if __name__ == '__main__':
    main()
