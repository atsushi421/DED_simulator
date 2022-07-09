import networkx as nx


class SubDAG:

    def __init__(
        self,
        dag: nx.DiGraph
    ) -> None:
        self._dag = dag

    @property
    def dag(self):
        return self._dag
