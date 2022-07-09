import networkx as nx
from src.job_generator import JobGenerator
from src.sub_dag import SubDAG


class TestJobGenerator:

    def test_get_ready_nodes(self, mocker):
        sub_dag_mock = mocker.Mock(spec=SubDAG)
        dag_mock = mocker.Mock(spec=nx.DiGraph)
        mocker.patch.object(dag_mock,
                            'succ',
                            {1: [1, 2]})
        mocker.patch.object(dag_mock,
                            'pred',
                            {1: [1, 4], 2: [1, 3]})
        mocker.patch.object(sub_dag_mock,
                            'dag',
                            dag_mock)

        ready_nodes = JobGenerator._get_ready_nodes(
            sub_dag_mock,
            {1, 2, 3},
            1
        )
        assert ready_nodes == [2]

    def test_generate(self, EG_divided):
        JobGenerator.generate(EG_divided)
        # TODO
