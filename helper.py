import pandas as pd


def df_tostring(df, rows=5):
    return f'  shape: {df.shape}\n' \
           f'  dataframe (first 5 rows):\n{df.head(rows).to_string()}\n'


def nodes_to_dataframe(g):
    return pd.DataFrame.from_dict(dict(g.nodes(data=True))).transpose()
