from src.dag_divider import DAGDivider


class TestDAGDivider:

    def test_divide_dag(self, EG):
        sub_dag_list = DAGDivider.divide_dag(EG)
        for sub_dag_i in sub_dag_list:
            assert set(sub_dag_i.dag.nodes) in ({0, 2},
                                                {1, 3},
                                                {4},
                                                {5, 6})
            for si, ti in sub_dag_i.dag.edges:
                assert not sub_dag_i.dag.edges[si, ti]['is_update']
