import json
import networkx as nx
import pandas as pd


class FileDriverBase:
    file_extension = ''

    @staticmethod
    def writer(file_content, file_path, kwargs):
        pass

    @staticmethod
    def reader(file_path, kwargs):
        pass

    @staticmethod
    def __tostring(file_content):
        return ''


class PandasFileDriver(FileDriverBase):
    file_extension = 'csv'

    @staticmethod
    def writer(df, file_path, kwargs):
        df.to_csv(file_path, **kwargs)
        return PandasFileDriver.__tostring(df, 5)

    @staticmethod
    def reader(file_path, kwargs):
        return pd.read_csv(file_path, **kwargs)

    @staticmethod
    def __tostring(df, rows=None):
        return f'  shape: {df.shape}\n' \
            f'  dataframe ({"first " + str(rows) if rows else "all"} rows):\n{df.head(rows).to_string()}\n'


class JsonFileDriver(FileDriverBase):
    file_extension = 'json'

    @staticmethod
    def writer(json_content, file_path, kwargs):
        with open(file_path, 'w') as json_file:
            json.dump(json_content, json_file, **kwargs)
        return ''

    @staticmethod
    def reader(file_path, kwargs):
        with open(file_path) as json_file:
            json_content = json.load(json_file, **kwargs)
        return json_content


class NetworkxFileDriver(FileDriverBase):
    file_extension = 'gexf'

    @staticmethod
    def writer(graph, file_path, kwargs):
        nx.write_gexf(graph, file_path, **kwargs)
        return NetworkxFileDriver.__tostring(graph, 5, 5)

    @staticmethod
    def reader(file_path, kwargs):
        graph = nx.read_gexf(file_path, **kwargs)
        for n in graph.nodes(data=True):
            del n[1]['label']

        for e in graph.edges(data=True):
            del e[2]['id']
            e[2]['weight'] = int(e[2]['weight'])

        return graph

    @staticmethod
    def __tostring(graph, nodes=None, edges=None):
        return f'  shape: ({len(graph.nodes)}, {len(graph.edges)})\n' \
            f'  nodes ({"first " + str(nodes) if nodes else "all"} nodes): ' \
            f'{str(list(graph.nodes(data=True))[:nodes])}\n' \
            f'  edges ({"first " + str(edges) if edges else "all"} edges): ' \
            f'{str(list(graph.edges(data=True))[:edges])}\n'


file_models = {
    'csv': PandasFileDriver(),
    'json': JsonFileDriver(),
    'gexf': NetworkxFileDriver()
}
