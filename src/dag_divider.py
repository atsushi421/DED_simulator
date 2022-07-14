import copy
from typing import List

import networkx as nx

from src.dag import DAG
from src.sub_dag import SubDAG


class DAGDivider:
    _temp_sub_dag = SubDAG()

    @staticmethod
    def divide(
        dag: DAG
    ) -> List[SubDAG]:
        sub_dags: List[SubDAG] = []

        for timer_i in dag.timer_nodes:
            DAGDivider._temp_sub_dag.add_node(
                timer_i, **dag.nodes[timer_i])
            DAGDivider._search_succs(dag, timer_i)
            DAGDivider._temp_sub_dag.initialize()
            sub_dags.append(copy.deepcopy(DAGDivider._temp_sub_dag))
            DAGDivider._temp_sub_dag = SubDAG()

        return sub_dags

    @staticmethod
    def _search_succs(
        dag: DAG,
        node_i: int
    ) -> None:
        if succ_tri := dag.get_succ_tri(node_i):
            for succ_tri_i in succ_tri:
                DAGDivider._temp_sub_dag.add_node(
                    succ_tri_i, **dag.nodes[succ_tri_i])
                DAGDivider._temp_sub_dag.add_edge(
                    node_i, succ_tri_i,
                    **dag.edges[node_i, succ_tri_i]
                )
                DAGDivider._search_succs(dag, succ_tri_i)
