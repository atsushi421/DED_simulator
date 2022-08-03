import os
import sys
from typing import List

import pandas as pd
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
        RS.initialize(1.2)
        RS.sub_dags = DAGDivider.divide(RS)
        RS.set_num_trigger()
        jitter_generator = JitterGenerator(
            f'{os.path.dirname(__file__)}/../reference_system_exec_jitter_multi_8core_8192.yaml',
            "1.5"
        )
        jitter_generator.set_wcet(RS)
        JobGenerator.generate(RS)
        RS.jld = JLDAnalyzer.analyze(RS, 'proposed', 1.2)
        RS.reflect_jobs_in_dag()
        LaxityCalculator.calculate(RS)
        early_detection_df = RS.get_laxity_df()
        jitter_generator.generate_exec_jitter(RS)

        processor = MultiCoreProcessor(8)
        scheduler = Scheduler('LLF', RS, processor, 1.2, early_detection_df)
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

    def test_pop_highest_priority_job_LLF(self, mocker, EG_scheduler):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'laxity', 10)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'laxity', 30)
        job_mock2 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock2, 'laxity', 20)

        ready_jobs = [job_mock0, job_mock1, job_mock2]

        assert [EG_scheduler._pop_highest_priority_job(ready_jobs)
                for _ in range(3)] == [job_mock0, job_mock2, job_mock1]

    def test_pop_highest_priority_job_EDF(self, EG_scheduler):
        ready_jobs = []
        EG_scheduler._update_ready_jobs(ready_jobs)
        EG_scheduler._algorithm = 'EDF'

        pops = [EG_scheduler._pop_highest_priority_job(ready_jobs)
                for _ in range(4)]
        assert pops[0].node_i == 4
        assert pops[1].node_i == 0
        assert pops[2].node_i == 5
        assert pops[3].node_i == 1

    def test_trigger_succs(self, EG_scheduler):
        ready_jobs = []
        EG_scheduler._update_ready_jobs(ready_jobs)
        head = EG_scheduler._pop_highest_priority_job(ready_jobs)
        idle_core = EG_scheduler._processor.get_idle_core()
        idle_core.allocate(head)

        for _ in range(14):
            now_finish_jobs = EG_scheduler._advance_time()
        EG_scheduler._trigger_succs(now_finish_jobs)

        assert not EG_scheduler._processor.cores[0].proc_job
        assert EG_scheduler._processor.cores[0].remain == 0
        assert (EG_scheduler._dag.nodes[6]['jobs'][0].tri_time
                == 14+7)

    def test_early_detection(self, EG_scheduler, mocker):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'node_i', 0)
        mocker.patch.object(job_mock0, 'job_i', 0)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'node_i', 0)
        mocker.patch.object(job_mock1, 'job_i', 1)
        job_mock2 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock2, 'node_i', 0)
        mocker.patch.object(job_mock2, 'job_i', 2)
        EG_scheduler._dag.nodes[0]['jobs'][1] = job_mock1
        EG_scheduler._dag.nodes[0]['jobs'][2] = job_mock2

        EG_scheduler._early_detection_df = pd.DataFrame(
            data=[{'job0': sys.maxsize,
                   'job1': sys.maxsize,
                   'job2': 20}],
            index=['node0']
        )

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
        finish_jobs = [job_mock0, job_mock1]

        EG_scheduler._current_time = 186
        assert not EG_scheduler._check_dfc(job_mock2, finish_jobs)

        EG_scheduler._current_time = 185
        assert EG_scheduler._check_dfc(job_mock2, finish_jobs)

    def test_get_update_ts_node_no_middle_join(self, mocker):
        sub_dag = SubDAG()
        sub_dag.add_nodes_from([0, 1, 2, 3, 4, 5])
        for node_i in sub_dag.nodes:
            sub_dag.nodes[node_i]['is_join'] = False
        sub_dag.add_edge(0, 1)
        sub_dag.add_edge(1, 2)
        sub_dag.add_edge(2, 3)
        sub_dag.add_edge(3, 4)
        sub_dag.add_edge(4, 5)
        sub_dag.head = 0

        tail_job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(tail_job_mock, 'node_i', 5)

        mocker.patch('src.scheduler.Scheduler.__init__', return_value=None)
        scheduler = Scheduler()

        # test
        ts_node = scheduler._get_update_ts_node(sub_dag, tail_job_mock)
        assert ts_node == 0

    def test_get_update_ts_node_middle_join(self, mocker):
        sub_dag = SubDAG()
        sub_dag.add_nodes_from([0, 1, 2, 3, 4, 5])
        for node_i in sub_dag.nodes:
            if node_i == 3:
                sub_dag.nodes[node_i]['is_join'] = True
                continue
            sub_dag.nodes[node_i]['is_join'] = False
        sub_dag.add_edge(0, 1)
        sub_dag.add_edge(1, 2)
        sub_dag.add_edge(2, 3)
        sub_dag.add_edge(3, 4)
        sub_dag.add_edge(4, 5)
        sub_dag.head = 0

        tail_job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(tail_job_mock, 'node_i', 5)

        mocker.patch('src.scheduler.Scheduler.__init__', return_value=None)
        scheduler = Scheduler()

        # test
        ts_node = scheduler._get_update_ts_node(sub_dag, tail_job_mock)
        assert ts_node == 3

    def test_get_timestamp(self, mocker):
        sub_dag = SubDAG()
        sub_dag.add_nodes_from([0, 1, 2, 3, 4, 5])
        for node_i in sub_dag.nodes:
            sub_dag.nodes[node_i]['is_join'] = False
        sub_dag.add_edge(0, 1)
        sub_dag.add_edge(1, 2)
        sub_dag.add_edge(2, 3)
        sub_dag.add_edge(3, 4)
        sub_dag.add_edge(4, 5)
        sub_dag.head = 0
        mocker.patch('src.scheduler.Scheduler._get_containing_sub_dag',
                     return_value=sub_dag)

        tail_job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(tail_job_mock, 'node_i', 5)
        mocker.patch.object(tail_job_mock, 'job_i', 3)

        mocker.patch('src.scheduler.Scheduler.__init__', return_value=None)
        scheduler = Scheduler()

        finish_jobs: List[Job] = []
        finish_job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(finish_job_mock0, 'node_i', 0)
        mocker.patch.object(finish_job_mock0, 'job_i', 3)
        mocker.patch.object(finish_job_mock0, 'tri_time', 150)
        finish_jobs.append(finish_job_mock0)
        finish_job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(finish_job_mock1, 'node_i', 1)
        mocker.patch.object(finish_job_mock1, 'job_i', 3)
        finish_jobs.append(finish_job_mock1)
        finish_job_mock2 = mocker.Mock(spec=Job)
        mocker.patch.object(finish_job_mock2, 'node_i', 2)
        mocker.patch.object(finish_job_mock2, 'job_i', 3)
        finish_jobs.append(finish_job_mock2)
        finish_job_mock3 = mocker.Mock(spec=Job)
        mocker.patch.object(finish_job_mock3, 'node_i', 3)
        mocker.patch.object(finish_job_mock3, 'job_i', 3)
        finish_jobs.append(finish_job_mock3)
        finish_job_mock4 = mocker.Mock(spec=Job)
        mocker.patch.object(finish_job_mock4, 'node_i', 4)
        mocker.patch.object(finish_job_mock4, 'job_i', 3)
        finish_jobs.append(finish_job_mock4)
        finish_job_mock5 = mocker.Mock(spec=Job)
        mocker.patch.object(finish_job_mock5, 'node_i', 0)
        mocker.patch.object(finish_job_mock5, 'job_i', 2)
        finish_jobs.append(finish_job_mock4)

        # test
        ts = scheduler._get_timestamp(tail_job_mock, finish_jobs)
        assert ts == 150
