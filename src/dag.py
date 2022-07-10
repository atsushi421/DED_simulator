from typing import List, Optional

from networkx import DiGraph

from src.sub_dag import SubDAG


class DAG(DiGraph):

    def __init__(self) -> None:
        super().__init__()

    @property
    def sub_dags(self):
        return self._sub_dags

    @sub_dags.setter
    def sub_dags(
        self,
        sub_dags: List[SubDAG]
    ):
        self._sub_dags = sub_dags

    def initialize(self) -> None:
        self.timer_nodes = [node_i for node_i in self.nodes
                            if 'period' in self.nodes[node_i].keys()]
        self.update_edges = [(si, ti) for si, ti in self.edges
                             if self.edges[si, ti]['is_update']]
        self.trigger_edges = list(set(self.edges) - set(self.update_edges))
        self.hp = self._calc_hp()

    def _calc_hp(self) -> int:
        periods = [self.nodes[node_i]['period']
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
        return [succ_i for succ_i in self.succ[node_i]
                if (node_i, succ_i) in self.trigger_edges]
