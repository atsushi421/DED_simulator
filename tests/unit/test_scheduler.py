import os
import sys

from src.dag import DAG
from src.dag_divider import DAGDivider
from src.dag_reader import DAGReader
from src.jitter_generator import JitterGenerator
from src.jld_analyzer import JLDAnalyzer
from src.job_generator import Job, JobGenerator
from src.laxity_calculator import LaxityCalculator
from src.multi_core_processor import MultiCoreProcessor
from src.scheduler import ScheduleLogger, Scheduler
from src.sub_dag import SubDAG


class TestScheduleLogger:

    def test_write_allocate(self, mocker):
        job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock, 'node_i', 0)
        mocker.patch.object(job_mock, 'job_i', 0)
        mocker.patch.object(job_mock, 'tri_time', 0)
        mocker.patch.object(job_mock, 'deadline', 100)

        logger = ScheduleLogger(4)
        logger.write_allocate(0, job_mock, 0, 23)
        assert len(logger._sched_log['taskSet']) == 1
        assert isinstance(logger._sched_log['taskSet'][0], dict)

    def test_dump_sched_log(self, mocker):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'node_i', 0)
        mocker.patch.object(job_mock0, 'job_i', 0)
        mocker.patch.object(job_mock0, 'tri_time', 0)
        mocker.patch.object(job_mock0, 'deadline', 100)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'node_i', 1)
        mocker.patch.object(job_mock1, 'job_i', 0)
        mocker.patch.object(job_mock1, 'tri_time', 10)
        mocker.patch.object(job_mock1, 'deadline', 100)
        job_mock2 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock2, 'node_i', 2)
        mocker.patch.object(job_mock2, 'job_i', 0)
        mocker.patch.object(job_mock2, 'tri_time', 30)
        mocker.patch.object(job_mock2, 'deadline', 100)

        logger = ScheduleLogger(4)
        logger.write_allocate(0, job_mock0, 0, 23)
        logger.write_allocate(1, job_mock1, 10, 42)
        logger.write_allocate(2, job_mock2, 30, 64)
        logger.write_makespan(64)
        logger.dump_sched_log(
            f'{os.path.dirname(__file__)}/../test_sched_log.json')
        assert os.path.exists(
            f'{os.path.dirname(__file__)}/../test_sched_log.json')
        assert os.path.isfile(
            f'{os.path.dirname(__file__)}/../test_sched_log.json')

    def test_dump_dm_log(self, mocker):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'node_i', 0)
        mocker.patch.object(job_mock0, 'job_i', 0)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'node_i', 1)
        mocker.patch.object(job_mock1, 'job_i', 0)

        logger = ScheduleLogger(4)
        logger.write_early_detection(10, job_mock0)
        logger.write_deadline_miss('e2e deadline', 20, job_mock1)
        logger.dump_dm_log(
            f'{os.path.dirname(__file__)}/../test_dm_log.yaml')
        assert os.path.exists(
            f'{os.path.dirname(__file__)}/../test_dm_log.yaml')
        assert os.path.isfile(
            f'{os.path.dirname(__file__)}/../test_dm_log.yaml')


class TestScheduler:

    # def test_remove_unnecessary_jobs(self, EG_calculated):
    #     Scheduler._remove_unnecessary_jobs(EG_calculated)

    #     for node_i in EG_calculated.nodes:
    #         if node_i == 0:
    #             assert len(EG_calculated.nodes[node_i]['jobs']) == 10
    #         if node_i == 1:
    #             assert len(EG_calculated.nodes[node_i]['jobs']) == 3
    #         if node_i == 2:
    #             assert len(EG_calculated.nodes[node_i]['jobs']) == 10
    #         if node_i == 3:
    #             assert len(EG_calculated.nodes[node_i]['jobs']) == 3
    #         if node_i == 4:
    #             assert len(EG_calculated.nodes[node_i]['jobs']) == 15
    #         if node_i == 5:
    #             assert len(EG_calculated.nodes[node_i]['jobs']) == 6
    #         if node_i == 6:
    #             assert len(EG_calculated.nodes[node_i]['jobs']) == 6

    def test_schedule_01(self, EG_scheduler):
        EG_scheduler.schedule()
        logger = EG_scheduler.create_logger()
        logger.dump_sched_log(
            f'{os.path.dirname(__file__)}/../test_sched_log.json')
        assert os.path.exists(
            f'{os.path.dirname(__file__)}/../test_sched_log.json')
        assert os.path.isfile(
            f'{os.path.dirname(__file__)}/../test_sched_log.json')
        logger.dump_dm_log(
            f'{os.path.dirname(__file__)}/../test_dm_log.yaml')
        assert os.path.exists(
            f'{os.path.dirname(__file__)}/../test_dm_log.yaml')
        assert os.path.isfile(
            f'{os.path.dirname(__file__)}/../test_dm_log.yaml')

    def test_schedule_02(self):
        RS = DAGReader._read_dot(
            f'{os.path.dirname(__file__)}/../referenceSystem.dot')
        RS.sub_dags = DAGDivider.divide(RS)
        RS.set_num_trigger()
        jitter_generator = JitterGenerator(
            f'{os.path.dirname(__file__)}/../reference_system_exec_jitter_multi_8core_8192.yaml',
            "1.1"
        )
        jitter_generator.set_wcet(RS)
        JobGenerator.generate(RS)
        RS.jld = JLDAnalyzer.analyze(RS, 'proposed', 2.0)
        RS.reflect_jobs_in_dag()
        LaxityCalculator.calculate(RS)
        jitter_generator.generate_exec_jitter(RS)

        processor = MultiCoreProcessor(8)
        scheduler = Scheduler('LLF', RS, processor, 1.7, True)
        scheduler.schedule()

    def test_get_containing_sub_dag(self, EG_scheduler):
        node0_job = EG_scheduler._dag.nodes[0]['jobs'][0]
        node0_sub_dag = EG_scheduler._get_containing_sub_dag(node0_job)
        assert node0_sub_dag == EG_scheduler._dag.sub_dags[0]

        node1_job = EG_scheduler._dag.nodes[1]['jobs'][0]
        node1_sub_dag = EG_scheduler._get_containing_sub_dag(node1_job)
        assert node1_sub_dag == EG_scheduler._dag.sub_dags[1]

        node5_job = EG_scheduler._dag.nodes[5]['jobs'][0]
        node5_sub_dag = EG_scheduler._get_containing_sub_dag(node5_job)
        assert node5_sub_dag == EG_scheduler._dag.sub_dags[3]

        node4_job = EG_scheduler._dag.nodes[4]['jobs'][0]
        node4_sub_dag = EG_scheduler._get_containing_sub_dag(node4_job)
        assert node4_sub_dag == EG_scheduler._dag.sub_dags[2]

    def test_update_ready_jobs(self, EG_scheduler):
        EG_scheduler._update_ready_jobs()

        assert len(EG_scheduler._ready_jobs) == 4
        for job in EG_scheduler._ready_jobs:
            assert job.tri_time == EG_scheduler._current_time

    def test_pop_highest_priority_job_LLF(self, mocker, EG_scheduler):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'laxity', 10)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'laxity', 30)
        job_mock2 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock2, 'laxity', 20)

        EG_scheduler._ready_jobs = [job_mock0, job_mock1, job_mock2]

        assert [EG_scheduler._pop_highest_priority_job()
                for _ in range(3)] == [job_mock0, job_mock2, job_mock1]

    def test_pop_highest_priority_job_EDF(self, EG_scheduler):
        EG_scheduler._update_ready_jobs()
        EG_scheduler._algorithm = 'EDF'

        pops = [EG_scheduler._pop_highest_priority_job() for _ in range(4)]
        assert pops[0].node_i == 4
        assert pops[1].node_i == 0
        assert pops[2].node_i == 5
        assert pops[3].node_i == 1

    def test_advance_time_01(self, EG_scheduler):
        EG_scheduler._update_ready_jobs()
        head = EG_scheduler._pop_highest_priority_job()
        idle_core = EG_scheduler._processor.get_idle_core()
        idle_core.allocate(head)

        for _ in range(14):
            EG_scheduler._advance_time()

        assert not EG_scheduler._processor.cores[0].proc_job
        assert EG_scheduler._processor.cores[0].remain == 0
        assert (EG_scheduler._dag.nodes[6]['jobs'][0].tri_time
                == 14+7)

    def test_advance_time_02(self, EG_scheduler, mocker):
        job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock, 'node_i', 6)
        mocker.patch.object(job_mock, 'exec', 11)
        mocker.patch.object(job_mock, 'deadline', 10)
        mocker.patch('src.dag.DAG.get_succ_tri',
                     return_value=None)
        idle_core = EG_scheduler._processor.get_idle_core()
        idle_core.allocate(job_mock)

        for _ in range(11):
            EG_scheduler._advance_time()
        assert EG_scheduler._dm_flag

    def test_early_detection(self, EG_scheduler, mocker):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'node_i', 0)
        mocker.patch.object(job_mock0, 'job_i', 0)
        mocker.patch.object(job_mock0, 'laxity', sys.maxsize)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'node_i', 0)
        mocker.patch.object(job_mock1, 'job_i', 1)
        mocker.patch.object(job_mock1, 'laxity', sys.maxsize)
        job_mock2 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock2, 'node_i', 0)
        mocker.patch.object(job_mock2, 'job_i', 2)
        mocker.patch.object(job_mock2, 'laxity', 20)
        EG_scheduler._dag.nodes[0]['jobs'][1] = job_mock1
        EG_scheduler._dag.nodes[0]['jobs'][2] = job_mock2

        EG_scheduler._current_time = 21
        assert not EG_scheduler._early_detection(job_mock0)

        EG_scheduler._current_time = 20
        assert EG_scheduler._early_detection(job_mock0)

    def test_check_dfc(self, mocker, EG_scheduler):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'node_i', 0)
        mocker.patch.object(job_mock0, 'job_i', 0)
        mocker.patch.object(job_mock0, 'rst', 100)
        mocker.patch.object(job_mock0, 'tri_time', 100)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'node_i', 1)
        mocker.patch.object(job_mock1, 'job_i', 0)
        mocker.patch.object(job_mock1, 'laxity', sys.maxsize)
        job_mock2 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock2, 'node_i', 2)
        mocker.patch.object(job_mock2, 'job_i', 0)
        mocker.patch.object(job_mock2, 'laxity', 20)

        dag = DAG()
        dag.add_node(0)
        dag.add_edge(0, 1)
        dag.add_edge(1, 2)
        sub_dag0 = SubDAG()
        sub_dag0.add_node(0)
        sub_dag0.add_node(1, is_join=False)
        sub_dag0.add_edge(0, 1)
        sub_dag0.head = job_mock0.node_i
        sub_dag0.period = 50
        sub_dag1 = SubDAG()
        sub_dag1.add_node(2)
        dag._sub_dags = [sub_dag0, sub_dag1]
        EG_scheduler._dag = dag
        EG_scheduler._finish_jobs = [job_mock0, job_mock1]

        EG_scheduler._current_time = 186
        assert not EG_scheduler._check_dfc(job_mock2)

        EG_scheduler._current_time = 185
        assert EG_scheduler._check_dfc(job_mock2)
