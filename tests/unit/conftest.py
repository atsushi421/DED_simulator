import os

import pytest
from src.dag import DAG
from src.dag_divider import DAGDivider
from src.dag_reader import DAGReader
from src.jld_analyzer import JLDAnalyzer
from src.job_generator import JobGenerator
from src.laxity_calculator import LaxityCalculator
from src.multi_core_processor import MultiCoreProcessor
from src.scheduler import Scheduler


@pytest.fixture
def EG() -> DAG:
    EG = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')

    return EG


@pytest.fixture
def EG_divided() -> DAG:
    EG_divided = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')
    EG_divided.sub_dags = DAGDivider.divide(EG_divided)

    return EG_divided


@pytest.fixture
def EG_job_generated() -> DAG:
    EG_job_generated = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')
    EG_job_generated.sub_dags = DAGDivider.divide(EG_job_generated)
    JobGenerator.generate(EG_job_generated)

    return EG_job_generated


@pytest.fixture
def EG_analyzed() -> DAG:
    EG_analyzed = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')
    EG_analyzed.sub_dags = DAGDivider.divide(EG_analyzed)
    JobGenerator.generate(EG_analyzed)
    EG_analyzed.jld = JLDAnalyzer.analyze(EG_analyzed, 'proposed', 1.7)
    EG_analyzed.reflect_jobs_in_dag()

    return EG_analyzed


@pytest.fixture
def EG_calculated() -> DAG:
    EG_calculated = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')
    EG_calculated.sub_dags = DAGDivider.divide(EG_calculated)
    JobGenerator.generate(EG_calculated)
    EG_calculated.jld = JLDAnalyzer.analyze(EG_calculated, 'proposed', 1.7)
    EG_calculated.reflect_jobs_in_dag()
    LaxityCalculator.calculate(EG_calculated)

    return EG_calculated


@pytest.fixture
def EG_scheduler():
    EG_calculated = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')
    EG_calculated.sub_dags = DAGDivider.divide(EG_calculated)
    JobGenerator.generate(EG_calculated)
    EG_calculated.jld = JLDAnalyzer.analyze(EG_calculated, 'proposed', 1.7)
    EG_calculated.reflect_jobs_in_dag()
    LaxityCalculator.calculate(EG_calculated)

    processor = MultiCoreProcessor(8)
    EG_scheduler = Scheduler('LLF', EG_calculated, processor, 1.7, True)

    return EG_scheduler
