import os

import pytest
from src.dag import DAG
from src.dag_reader import DAGReader


@pytest.fixture
def EG() -> DAG:
    EG = DAGReader._read_dot(
        f'{os.path.dirname(__file__)}/../example_dag.dot')
    return EG
