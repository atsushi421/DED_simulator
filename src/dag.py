from typing import List, Optional

import networkx as nx

from src.sub_dag import SubDAG


class DAG:

    def __init__(
        self,
        dag: nx.DiGraph
    ) -> None:
        self._raw_dag = dag
        self._timer_nodes = [node_i for node_i in self._raw_dag.nodes
                             if 'period' in self._raw_dag.nodes[node_i].keys()]
        self._update_edges = [(si, ti) for si, ti in self._raw_dag.edges
                              if self._raw_dag.edges[si, ti]['is_update']]
        self._trigger_edges = list(
            set(self.raw_dag.edges) - set(self.update_edges))
        self._hp = self._calc_hp()

    @property
    def raw_dag(self):
        return self._raw_dag

    @property
    def timer_nodes(self):
        return self._timer_nodes

    @property
    def update_edges(self):
        return self._update_edges

    @property
    def trigger_edges(self):
        return self._trigger_edges

    @property
    def hp(self):
        return self._hp

    @property
    def sub_dags(self):
        return self._sub_dags

    @sub_dags.setter
    def sub_dags(
        self,
        sub_dags: List[SubDAG]
    ):
        self._sub_dags = sub_dags

    def _calc_hp(self) -> int:
        periods = [self.raw_dag.nodes[node_i]['period']
                   for node_i in self.timer_nodes]

        greatest = max(periods)
        i = 1
        while True:
            for j in periods:
                if (greatest * i) % j != 0:
                    i += 1
                    break
            else:
                return greatest * i

    def get_succ_tri(
        self,
        node_i: int
    ) -> Optional[List[int]]:
        return [succ_i for succ_i in self._raw_dag.succ[node_i]
                if (node_i, succ_i) in self._trigger_edges] or None
