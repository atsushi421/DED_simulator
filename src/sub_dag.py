import networkx as nx


class SubDAG:

    def __init__(
        self,
        dag: nx.DiGraph
    ) -> None:
        self._dag = dag
        self._head = [v for v, d in self.dag.in_degree() if d == 0][0]
        self._tails = [v for v, d in self.dag.out_degree() if d == 0]
        self._period = self.dag.nodes[self.head]['period']

    @property
    def dag(self):
        return self._dag

    @property
    def head(self):
        return self._head

    @property
    def tails(self):
        return self._tails

    @property
    def period(self):
        return self._period

    @property
    def num_trigger(self):
        return self._num_trigger

    @num_trigger.setter
    def num_trigger(self, num_trigger: int):
        self._num_trigger = num_trigger
