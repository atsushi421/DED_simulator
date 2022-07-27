import os

from src.jld_analyzer import JLDAnalyzer
from src.laxity_calculator import LaxityCalculator


class TestLaxityDataFrame:

    def test_proposed(self, EG_job_generated):
        EG_job_generated.jld = JLDAnalyzer.analyze(
            EG_job_generated, 'proposed', 1.7)
        EG_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(EG_job_generated)

        df = EG_job_generated.get_laxity_df()
        df.to_csv(f'{os.path.dirname(__file__)}/../test_laxity_proposed.csv')

    def test_Igarashi(self, EG_job_generated):
        EG_job_generated.jld = JLDAnalyzer.analyze(
            EG_job_generated, 'Igarashi', 1.7)
        EG_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(EG_job_generated)

        df = EG_job_generated.get_laxity_df()
        df.to_csv(f'{os.path.dirname(__file__)}/../test_laxity_Igarashi.csv')

    def test_Saidi(self, EG_job_generated):
        EG_job_generated.jld = JLDAnalyzer.analyze(
            EG_job_generated, 'Saidi', 1.7)
        EG_job_generated.reflect_jobs_in_dag()
        LaxityCalculator.calculate(EG_job_generated)

        df = EG_job_generated.get_laxity_df()
        df.to_csv(f'{os.path.dirname(__file__)}/../test_laxity_Saidi.csv')
