import os

from src.jld_analyzer import JLDAnalyzer
from src.laxity_calculator import LaxityCalculator


class TestLaxityDataFrame:

    def test_proposed_EG(self, EG_job_generated):
        EG_job_generated.jld = JLDAnalyzer.analyze(
            EG_job_generated, 'proposed', 1.7)
        EG_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(EG_job_generated)

        df = EG_job_generated.get_laxity_df()
        df.to_csv(
            f'{os.path.dirname(__file__)}/../test_laxity_proposed_EG.csv')

    def test_proposed_AA(self, AA_job_generated):
        AA_job_generated.jld = JLDAnalyzer.analyze(
            AA_job_generated, 'proposed', 1.7)
        AA_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(AA_job_generated)

        df = AA_job_generated.get_laxity_df()
        df.to_csv(
            f'{os.path.dirname(__file__)}/../test_laxity_proposed_AA.csv')

    def test_Igarashi_EG(self, EG_job_generated):
        EG_job_generated.jld = JLDAnalyzer.analyze(
            EG_job_generated, 'Igarashi', 1.7)
        EG_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(EG_job_generated)

        df = EG_job_generated.get_laxity_df()
        df.to_csv(f'{os.path.dirname(__file__)}/../test_laxity_Igarashi.csv')

    def test_Igarashi_AA(self, AA_job_generated):
        AA_job_generated.jld = JLDAnalyzer.analyze(
            AA_job_generated, 'Igarashi', 1.7)
        AA_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(AA_job_generated)

        df = AA_job_generated.get_laxity_df()
        df.to_csv(
            f'{os.path.dirname(__file__)}/../test_laxity_Igarashi_AA.csv')

    def test_Saidi_EG(self, EG_job_generated):
        EG_job_generated.jld = JLDAnalyzer.analyze(
            EG_job_generated, 'Saidi', 1.7)
        EG_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(EG_job_generated)

        df = EG_job_generated.get_laxity_df()
        df.to_csv(f'{os.path.dirname(__file__)}/../test_laxity_Saidi.csv')

    def test_Saidi_AA(self, AA_job_generated):
        AA_job_generated.jld = JLDAnalyzer.analyze(
            AA_job_generated, 'Saidi', 1.7)
        AA_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(AA_job_generated)

        df = AA_job_generated.get_laxity_df()
        df.to_csv(f'{os.path.dirname(__file__)}/../test_laxity_Saidi_AA.csv')
