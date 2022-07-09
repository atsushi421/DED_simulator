from src.dag_divider import DAGDivider


class TestDAGDivider:

    def test_divide_dag(self, EG):
        sub_dag_list = DAGDivider.divide(EG)
        for sub_dag_i in sub_dag_list:
            assert set(sub_dag_i.dag.nodes) in ({0, 2},
                                                {1, 3},
                                                {4},
                                                {5, 6})
            for si, ti in sub_dag_i.dag.edges:
                assert not sub_dag_i.dag.edges[si, ti]['is_update']
                assert (sub_dag_i.dag.edges[si, ti]['comm']
                        == EG.raw_dag.edges[si, ti]['comm'])

            for node_i in sub_dag_i.dag.nodes:
                if period := sub_dag_i.dag.nodes[node_i].get('period'):
                    assert period == EG.raw_dag.nodes[node_i]['period']
                assert (sub_dag_i.dag.nodes[node_i]['exec']
                        == EG.raw_dag.nodes[node_i]['exec'])
                assert (sub_dag_i.dag.nodes[node_i]['is_join']
                        == EG.raw_dag.nodes[node_i]['is_join'])
