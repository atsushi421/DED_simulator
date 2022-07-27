from src.dag import DAG


class TestDAG:

    def test_initialize(self):
        dag = DAG()
        dag.add_node(0, period=60)
        dag.add_node(1, period=120)
        dag.add_nodes_from([2, 3, 4])
        dag.add_edge(0, 2, is_update=False)
        dag.add_edge(1, 3, is_update=False)
        dag.add_edge(2, 4, is_update=True)
        dag.add_edge(3, 4, is_update=False)
        dag.initialize(e2e_deadline_tightness=2.0)

        assert dag.exit_i == 4
        assert dag.timer_nodes == [0, 1]
        assert dag.update_edges == [(2, 4)]
        assert dag.trigger_edges == [(0, 2), (1, 3), (3, 4)]
        assert dag.nodes[dag.exit_i]['deadline'] == int(120 * 2.0)
        assert dag.hp == 120

    def test_get_succ_tri(self, EG):
        assert EG.get_succ_tri(0) == [2]
        assert EG.get_succ_tri(1) == [3]
        assert not EG.get_succ_tri(2)
        assert not EG.get_succ_tri(3)
        assert not EG.get_succ_tri(4)
        assert EG.get_succ_tri(5) == [6]
        assert not EG.get_succ_tri(6)

    def test_calc_hp(self, EG):
        assert EG.hp == 300
