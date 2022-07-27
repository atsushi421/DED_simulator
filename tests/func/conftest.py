import os

import pytest
from src.dag import DAG
from src.dag_divider import DAGDivider
from src.dag_reader import DAGReader
from src.job_generator import JobGenerator


@pytest.fixture
def EG_job_generated() -> DAG:
    EG_job_generated = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')
    EG_job_generated.initialize(1.0)
    EG_job_generated.sub_dags = DAGDivider.divide(EG_job_generated)
    EG_job_generated.set_num_trigger()
    JobGenerator.generate(EG_job_generated)

    return EG_job_generated
