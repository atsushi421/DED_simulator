import copy
from typing import List

import networkx as nx

from src.dag import DAG
from src.sub_dag import SubDAG


class DAGDivider:
    _temp_dag: nx.DiGraph = None

    @staticmethod
    def divide_dag(
        dag: DAG
    ) -> List[SubDAG]:
        sub_dag_list: List[SubDAG] = []

        DAGDivider._temp_dag = nx.DiGraph()
        for timer_i in dag.timer_nodes:
            DAGDivider._temp_dag.add_node(timer_i)
            DAGDivider._search_succs(dag, timer_i)
            sub_dag_list.append(SubDAG(copy.deepcopy(DAGDivider._temp_dag)))
            DAGDivider._temp_dag = nx.DiGraph()

        return sub_dag_list

    @staticmethod
    def _search_succs(
        dag: DAG,
        node_i: int
    ) -> None:
        if succ_tri := dag.get_succ_tri(node_i):
            DAGDivider._temp_dag.add_nodes_from(succ_tri)
            for succ_tri_i in succ_tri:
                DAGDivider._search_succs(dag, succ_tri_i)
