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

    @property
    def jld(self):
        return self._jld

    @jld.setter
    def jld(
        self,
        jld
    ):
        self._jld = jld

    def initialize(self) -> None:
        self.exit_i = [v for v, d in self.out_degree() if d == 0][0]
        self.timer_nodes = [node_i for node_i in self.nodes
                            if 'period' in self.nodes[node_i].keys()]
        self.update_edges = [(si, ti) for si, ti in self.edges
                             if self.edges[si, ti]['is_update']]
        self.trigger_edges = list(set(self.edges) - set(self.update_edges))
        self._set_deadline()
        self.hp = self._calc_hp()

    def _set_deadline(self) -> None:
        max_period = max([self.nodes[node_i]['period']
                          for node_i in self.timer_nodes])
        self.nodes[self.exit_i]['deadline'] = max_period

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

    def set_num_trigger(self) -> None:
        for sub_dag in self.sub_dags:
            sub_dag.num_trigger = int(self.hp / sub_dag.period)
            for node_i in sub_dag.nodes:
                self.nodes[node_i]['num_trigger'] = sub_dag.num_trigger

    def reflect_jobs_in_dag(self) -> None:
        for sub_dag in self.sub_dags:
            for node_i in sub_dag.nodes:
                self.nodes[node_i]['jobs'] = sub_dag.nodes[node_i]['jobs']

    def get_total_utilization(
        self
    ) -> float:
        total_utilization = 0

        for sub_dag in self.sub_dags:
            sum_sub_dag_utilization = 0
            num_trigger = self.nodes[sub_dag.head]['num_trigger']

            for job_i in range(num_trigger):
                sum_exec = 0

                for node_i in sub_dag.nodes:
                    sum_exec += sub_dag.nodes[node_i]['jobs'][job_i].exec

                sum_sub_dag_utilization += sum_exec / sub_dag.period

            ave_sub_dag_utilization = sum_sub_dag_utilization / num_trigger
            total_utilization += ave_sub_dag_utilization

        return total_utilization
