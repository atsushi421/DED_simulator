class TestDAG:

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
