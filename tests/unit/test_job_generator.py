from src.job_generator import JobGenerator
from src.sub_dag import SubDAG


class TestJobGenerator:

    def test_get_ready_nodes(self, mocker):
        sub_dag_mock = mocker.Mock(spec=SubDAG)
        mocker.patch.object(sub_dag_mock,
                            'succ',
                            {1: [1, 2]})
        mocker.patch.object(sub_dag_mock,
                            'pred',
                            {1: [1, 4], 2: [1, 3]})

        ready_nodes = JobGenerator._get_ready_nodes(
            sub_dag_mock,
            {1, 2, 3},
            1
        )
        assert ready_nodes == [2]

    def test_generate(self, EG_divided):
        JobGenerator.generate(EG_divided)

        for exit_job in EG_divided.sub_dags[3].nodes[6]['jobs']:
            assert (exit_job.deadline
                    == EG_divided.nodes[6]['deadline'] * (exit_job.job_i+1))
