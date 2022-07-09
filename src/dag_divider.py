import copy
from typing import List

import networkx as nx

from src.dag import DAG
from src.sub_dag import SubDAG


class DAGDivider:
    _temp_dag: nx.DiGraph = None

    @staticmethod
    def divide(
        dag: DAG
    ) -> List[SubDAG]:
        sub_dags: List[SubDAG] = []

        DAGDivider._temp_dag = nx.DiGraph()
        for timer_i in dag.timer_nodes:
            DAGDivider._temp_dag.add_node(
                timer_i, **dag.raw_dag.nodes[timer_i])
            DAGDivider._search_succs(dag, timer_i)
            sub_dags.append(
                SubDAG(copy.deepcopy(DAGDivider._temp_dag)))
            DAGDivider._temp_dag = nx.DiGraph()

        return sub_dags

    @staticmethod
    def _search_succs(
        dag: DAG,
        node_i: int
    ) -> None:
        if succ_tri := dag.get_succ_tri(node_i):
            for succ_tri_i in succ_tri:
                DAGDivider._temp_dag.add_node(
                    succ_tri_i, **dag.raw_dag.nodes[succ_tri_i])
                DAGDivider._temp_dag.add_edge(
                    node_i, succ_tri_i,
                    **dag.raw_dag.edges[node_i, succ_tri_i]
                )
                DAGDivider._search_succs(dag, succ_tri_i)
