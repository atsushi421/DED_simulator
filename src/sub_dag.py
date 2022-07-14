from networkx import DiGraph


class SubDAG(DiGraph):

    def __init__(
        self,
    ) -> None:
        super().__init__()

    def initialize(self) -> None:
        self.head = [v for v, d in self.in_degree() if d == 0][0]
        self.tails = [v for v, d in self.out_degree() if d == 0]
        self.period = self.nodes[self.head]['period']
