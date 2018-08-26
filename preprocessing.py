import pandas as pd
import config


def main():
    print('# PRE-PROCESSING')

    df_edges = load_csv()
    df_edges = drop_columns(df_edges)
    df_edges = merge_duplicates(df_edges)
    save_csv(df_edges)


def load_csv():
    print('## LOAD CSV')

    df = pd.read_csv(config.IO.csvData, dtype=config.IO.csvData_dtype)

    print(f'  shape: {df.shape}\n'
          f'  dataframe (first 5 rows):\n{df.head(5).to_string()}\n\n')

    return df


def drop_columns(df_edges):
    print('## DROP COLUMNS')

    df_edges = df_edges.drop(columns=['cod', 'user_from_fav_count', 'user_rt_fav_count', 'text'], axis=1)
    df_edges.rename(columns={'user_from_name': 'Source', 'user_to_name': 'Target', 'weights': 'Weight'}, inplace=True)

    print(f'  shape: {df_edges.shape}\n'
          f'  dataframe (first 5 rows):\n{df_edges.head(5).to_string()}\n\n')

    return df_edges


def merge_duplicates(df_edges):
    print('## MERGE DUPLICATES')

    df_edges.Source = df_edges.Source.str.lower()
    df_edges.Target = df_edges.Target.str.lower()

    df_edges_duplicates = df_edges[df_edges.duplicated(subset=['Source', 'Target'], keep='first')]
    df_edges = df_edges.groupby(['Source', 'Target']).sum().reset_index()

    print(f'  number of duplicates: {df_edges_duplicates.shape}\n'
          f'  shape after: {df_edges.shape}\n'
          f'  difference: {df_edges.shape[0] - df_edges_duplicates.shape[0]}\n'
          f'  duplicates (first 5):\n{df_edges_duplicates.head(5).to_string()}\n\n')

    return df_edges


def save_csv(df_edges):
    print('## SAVE CSV')

    df_edges.to_csv(config.IO.csvEdges_PP, index=False)

    print(f'  shape: {df_edges.shape}\n'
          f'  dataframe (first 5 rows):\n{df_edges.head(5).to_string()}\n\n')


if __name__ == '__main__':
    main()
