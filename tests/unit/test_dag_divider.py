from src.dag_divider import DAGDivider


class TestDAGDivider:

    def test_divide_dag(self, EG):
        sub_dags = DAGDivider.divide(EG)
        for sub_dag in sub_dags:
            assert set(sub_dag.nodes) in ({0, 2},
                                          {1, 3},
                                          {4},
                                          {5, 6})
            for si, ti in sub_dag.edges:
                assert not sub_dag.edges[si, ti]['is_update']
                assert (sub_dag.edges[si, ti]['comm']
                        == EG.edges[si, ti]['comm'])

            for node_i in sub_dag.nodes:
                if period := sub_dag.nodes[node_i].get('period'):
                    assert period == EG.nodes[node_i]['period']
                assert (sub_dag.nodes[node_i]['exec']
                        == EG.nodes[node_i]['exec'])
                assert (sub_dag.nodes[node_i]['is_join']
                        == EG.nodes[node_i]['is_join'])
