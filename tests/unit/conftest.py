import os

import pytest
from src.dag import DAG
from src.dag_divider import DAGDivider
from src.dag_reader import DAGReader


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
