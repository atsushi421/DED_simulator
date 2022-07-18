from networkx import DiGraph


class SubDAG(DiGraph):

    def __init__(
        self,
    ) -> None:
        super().__init__()

    @property
    def num_trigger(self):
        return self._num_trigger

    @num_trigger.setter
    def num_trigger(
        self,
        num_trigger
    ):
        self._num_trigger = num_trigger

    def initialize(self) -> None:
        self.head = [v for v, d in self.in_degree() if d == 0][0]
        self.tails = [v for v, d in self.out_degree() if d == 0]
        self.period = self.nodes[self.head]['period']
