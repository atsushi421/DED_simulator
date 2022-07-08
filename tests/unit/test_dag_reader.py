import os

import pytest
from src.dag_reader import DAGReader


class TestDAGReader:

    def test_validate(self):
        DAGReader._validate('dot')

        with pytest.raises(NotImplementedError) as e:
            DAGReader._validate('yaml')
        assert len(str(e.value)) >= 1

    def test_read_dot(self):
        G = DAGReader._read_dot(
            f'{os.path.dirname(__file__)}/../example_dag.dot')

        assert G.number_of_nodes() == 7
        assert G.number_of_edges() == 6

        assert list(G.succ[0]) == [2]
        assert list(G.succ[1]) == [3]
        assert list(G.succ[2]) == [5]
        assert list(G.succ[3]) == [5]
        assert list(G.succ[4]) == [6]
        assert list(G.succ[5]) == [6]
        assert len(list(G.succ[6])) == 0

        assert len(list(G.pred[0])) == 0
        assert len(list(G.pred[1])) == 0
        assert list(G.pred[2]) == [0]
        assert list(G.pred[3]) == [1]
        assert len(list(G.pred[4])) == 0
        assert set(G.pred[5]) == {2, 3}
        assert set(G.pred[6]) == {4, 5}

        for node_i in G.nodes:
            if period := G.nodes[node_i].get('period'):
                assert isinstance(period, int)
            assert isinstance(G.nodes[node_i]['is_join'], bool)

        for s_i, t_i in G.edges:
            assert isinstance(G.edges[s_i, t_i]['is_update'], bool)
