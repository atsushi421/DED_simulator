import os

import networkx as nx

from src.dag import DAG


class DAGReader:
    _supported_ext = ['dot']

    @staticmethod
    def read(filepath: str) -> DAG:
        _, ext = os.path.splitext(filepath)
        DAGReader._validate(ext)

        if ext == 'dot':
            return DAGReader._read_dot(filepath)

    @staticmethod
    def _read_dot(filepath: str) -> DAG:
        tmp_dag = nx.drawing.nx_pydot.read_dot(filepath)
        tmp_dag = nx.DiGraph(tmp_dag)
        tmp_dag.remove_node('\\n')

        G = DAG()
        for node_i in tmp_dag.nodes:
            G.add_node(int(node_i), exec=int(tmp_dag.nodes[node_i]['exec']))
            if period := tmp_dag.nodes[node_i].get('period'):
                G.nodes[int(node_i)]['period'] = int(period)
            if 'True' in tmp_dag.nodes[node_i]['is_join']:
                G.nodes[int(node_i)]['is_join'] = True
            else:
                G.nodes[int(node_i)]['is_join'] = False
        for s, t in tmp_dag.edges:
            G.add_edge(int(s), int(t), comm=int(tmp_dag.edges[s, t]['comm']))
            if 'True' in tmp_dag.edges[s, t]['is_update']:
                G.edges[int(s), int(t)]['is_update'] = True
            else:
                G.edges[int(s), int(t)]['is_update'] = False

        G.initialize()

        return G

    @staticmethod
    def _validate(ext: str) -> None:
        if ext not in DAGReader._supported_ext:
            raise NotImplementedError(
                f'{DAGReader._supported_ext} are allowed.')
