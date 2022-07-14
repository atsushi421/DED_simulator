import pytest
from src.jld_analyzer import JLD, JLDAnalyzer


class TestJLDAnalyzer:

    def test_validate(self):
        JLDAnalyzer._validate('proposed')
        JLDAnalyzer._validate('Igarashi')
        JLDAnalyzer._validate('Saidi')

        with pytest.raises(NotImplementedError) as e:
            JLDAnalyzer._validate('method')
        assert len(str(e.value)) >= 1

    def test_analyze_in_sub_dag(self, EG_job_generated):
        jld = JLD()
        JLDAnalyzer._analyze_in_sub_dag(EG_job_generated, jld)

        sub_dag0 = EG_job_generated.sub_dags[0]
        for sj, tj in zip(sub_dag0.nodes[0]['jobs'],
                          sub_dag0.nodes[2]['jobs']):
            assert jld.get_succ_jobs(sj) == [tj]
            assert jld.get_pred_jobs(tj) == [sj]

        sub_dag1 = EG_job_generated.sub_dags[1]
        for sj, tj in zip(sub_dag1.nodes[1]['jobs'],
                          sub_dag1.nodes[3]['jobs']):
            assert jld.get_succ_jobs(sj) == [tj]
            assert jld.get_pred_jobs(tj) == [sj]

        sub_dag2 = EG_job_generated.sub_dags[2]
        for j in sub_dag2.nodes[4]['jobs']:
            assert not jld.get_succ_jobs(j)
            assert not jld.get_pred_jobs(j)

        sub_dag3 = EG_job_generated.sub_dags[3]
        for sj, tj in zip(sub_dag3.nodes[5]['jobs'],
                          sub_dag3.nodes[6]['jobs']):
            assert jld.get_succ_jobs(sj) == [tj]
            assert jld.get_pred_jobs(tj) == [sj]

    def test_analyze_Igarashi(self, EG_job_generated):
        jld = JLD()
        JLDAnalyzer._analyze_Igarashi(EG_job_generated, jld)
        # TODO

    def test_analyze_Saidi(self, EG_job_generated):
        jld = JLD()
        JLDAnalyzer._analyze_Saidi(EG_job_generated, jld)
        # TODO

    def test_analyze_proposed(self, EG_job_generated):
        jld = JLD()
        JLDAnalyzer._analyze_proposed(EG_job_generated, jld, 1.7)

        sub_dag0 = EG_job_generated.sub_dags[0]
        sub_dag3 = EG_job_generated.sub_dags[3]

        for job_i, tail_job in enumerate(sub_dag0.nodes[2]['jobs']):
            if job_i == 0:
                assert (sub_dag3.nodes[5]['jobs'][1]
                        in jld.get_succ_jobs(tail_job))
            if job_i == 1:
                assert not jld.get_succ_jobs(tail_job)
            if job_i == 2:
                assert (sub_dag3.nodes[5]['jobs'][2]
                        in jld.get_succ_jobs(tail_job))
            if job_i == 3:
                assert not jld.get_succ_jobs(tail_job)
            if job_i == 4:
                assert (sub_dag3.nodes[5]['jobs'][3]
                        in jld.get_succ_jobs(tail_job))
            if job_i == 5:
                assert (sub_dag3.nodes[5]['jobs'][4]
                        in jld.get_succ_jobs(tail_job))
            if job_i == 6:
                assert not jld.get_succ_jobs(tail_job)
            if job_i == 7:
                assert (sub_dag3.nodes[5]['jobs'][5]
                        in jld.get_succ_jobs(tail_job))
            if job_i == 8:
                assert not jld.get_succ_jobs(tail_job)
            if job_i == 9:
                assert (sub_dag3.nodes[5]['jobs'][6]
                        in jld.get_succ_jobs(tail_job))
