import copy
import json
from typing import Dict, List, Optional

import pandas as pd
import yaml

from src.dag import DAG
from src.job_generator import Job
from src.multi_core_processor import MultiCoreProcessor
from src.sub_dag import SubDAG

NO_LAXITY_CRITERIA = 9000000000000000000  # HACK


class ScheduleLogger:

    def __init__(
        self,
        num_cores: int
    ) -> None:
        self._dm_log: Dict[str, Dict] = {}
        self._sched_log = {'makespan': 0,
                           'coreNum': num_cores,
                           'taskSet': []}

    def write_makespan(
        self,
        makespan: int
    ) -> None:
        self._sched_log['makespan'] = makespan

    def write_allocate(
        self,
        core_i: int,
        job: Job,
        start_time: int,
        finish_time: int
    ) -> None:
        job_dict = {"coreID": core_i,
                    "taskID": job.node_i,
                    "jobID": job.job_i,
                    "releaseTime": job.tri_time,
                    "startTime": start_time,
                    "finishTime": finish_time}
        if job.deadline:
            job_dict["deadline"] = job.deadline

        self._sched_log['taskSet'].append(job_dict)

    def write_early_detection(
        self,
        detection_time: int,
        job: Job
    ) -> None:
        if 'early_detection' not in self._dm_log.keys():
            self._dm_log['early_detection'] = {
                'detection_time': detection_time,
                'node_i': job.node_i,
                'job_i': job.job_i
            }

    def write_deadline_miss(
        self,
        reason,
        deadline_miss_time: int,
        job: Job
    ) -> None:
        self._dm_log['deadline_miss'] = {
            'reason': reason,
            'deadline_miss_time': deadline_miss_time,
            'node_i': job.node_i,
            'job_i': job.job_i
        }

    def dump_sched_log(
        self,
        dest_path: str
    ) -> None:
        with open(dest_path, 'w') as f:
            json.dump(self._sched_log, f, indent=4)

    def dump_dm_log(
        self,
        dest_path: str
    ) -> None:
        with open(dest_path, 'w') as f:
            yaml.dump(self._dm_log, f)


class Scheduler:
    _supported_algorithm = ['EDF', 'LLF']

    def __init__(
        self,
        algorithm: str,
        dag: DAG,
        processor: MultiCoreProcessor,
        alpha: float,
        early_detection_df: pd.DataFrame
    ) -> None:
        self._validate(algorithm)

        self._algorithm = algorithm
        self._dag = dag
        self._processor = processor
        self._alpha = alpha
        self._early_detection_df = early_detection_df
        self._logger = ScheduleLogger(len(self._processor.cores))
        self._current_time = 0

    def schedule(
        self,
        write_sched_log: bool = False,
        print_debug_info: bool = False
    ) -> None:
        # Initialize_variables
        ready_jobs: List[Job] = []
        finish_jobs: List[Job] = []

        while self._current_time != self._dag.hp:
            self._update_ready_jobs(ready_jobs)
            if print_debug_info and ready_jobs:
                print('[ready_jobs]: '
                      f'{[f"<{j.node_i}, {j.job_i}>" for j in ready_jobs]}')

            # Wait
            if not ready_jobs or not self._processor.get_idle_core():
                now_finish_jobs = self._advance_time()
                if now_finish_jobs:
                    finish_jobs += now_finish_jobs
                    self._trigger_succs(now_finish_jobs)

                    # Check deadline miss
                    for job in now_finish_jobs:
                        if (job.node_i == self._dag.exit_i and
                                self._current_time > job.deadline):
                            self._logger.write_deadline_miss(
                                'e2e deadline', self._current_time, job)
                            break
                continue

            # Allocatable
            head = self._pop_highest_priority_job(ready_jobs)

            if not self._early_detection(head):
                self._logger.write_early_detection(self._current_time, head)

            if (head.job_i != 0 and head.is_join
                    and not self._check_dfc(head, finish_jobs)):
                if ((exit_i := self._dag.exit_i) in
                        set(self._get_containing_sub_dag(head).nodes)):
                    dm_exit_job = self._dag.nodes[exit_i]['jobs'][head.job_i]
                    self._logger.write_deadline_miss(
                        'dfc',
                        dm_exit_job.deadline,
                        dm_exit_job
                    )
                    break
                else:
                    continue

            # Allocate
            idle_core = self._processor.get_idle_core()
            assert idle_core
            idle_core.allocate(head)
            if write_sched_log:
                self._logger.write_allocate(
                    idle_core.core_i,
                    head,
                    self._current_time,
                    self._current_time + head.exec
                )
            if print_debug_info:
                print('[Allocate]: '
                      f'time={self._current_time} '
                      f'<{head.node_i}, {head.job_i}> '
                      f'laxity={head.laxity}')

        if self._logger:
            self._logger.write_makespan(self._current_time)

    def create_logger(self) -> ScheduleLogger:
        return self._logger

    def _get_containing_sub_dag(
        self,
        job: Job
    ) -> SubDAG:
        for sub_dag in self._dag.sub_dags:
            if job.node_i in set(sub_dag.nodes):
                sg = sub_dag
                break

        return sg

    def _get_update_ts_node(
        self,
        sub_dag: SubDAG,
        tail_job: Job
    ) -> int:
        node_i = tail_job.node_i
        while (node_i != sub_dag.head
                and not sub_dag.nodes[node_i]['is_join']):
            node_i = list(sub_dag.pred[node_i])[0]

        return node_i

    def _get_timestamp(
        self,
        tail_job: Job,
        finish_jobs: List[Job]
    ) -> int:
        sub_dag = self._get_containing_sub_dag(tail_job)
        update_ts_node = self._get_update_ts_node(sub_dag, tail_job)
        for finish_job in reversed(finish_jobs):
            if (finish_job.node_i == update_ts_node
                    and finish_job.job_i == tail_job.job_i):
                timestamp = finish_job.tri_time

        try:
            return timestamp
        except UnboundLocalError as e:
            print(
                f'[debug] finish_jobs: {[f"<{f.node_i}, {f.job_i}>" for f in finish_jobs]}')
            print(f'[debug] tail_job: <{tail_job.node_i}, {tail_job.job_i}>')
            raise(e)

    def _check_dfc(
        self,
        head: Job,
        finish_jobs: List[Job]
    ) -> bool:
        for tail_i in self._dag.pred[head.node_i]:
            for finish_job in reversed(finish_jobs):
                if finish_job.node_i == tail_i:  # finish_job is pred tail_job
                    dfc = int(
                        self._alpha
                        * self._get_containing_sub_dag(finish_job).period
                    )
                    if ((self._current_time
                         - self._get_timestamp(finish_job, finish_jobs))
                            > dfc):
                        # print(f'[debug] ~dfc~ <{finish_job.node_i}, {finish_job.job_i}>')
                        return False
                    else:
                        break

        return True

    def _early_detection(
        self,
        head: Job
    ) -> bool:
        def get_early_detection_time(job: Job) -> int:
            return (self._early_detection_df.at[f'node{job.node_i}',
                                                f'job{job.job_i}'])

        edt = get_early_detection_time(head)
        # if edt > NO_LAXITY_CRITERIA:
        #     next_job_i = head.job_i + 1
        #     try:
        #         while edt > NO_LAXITY_CRITERIA:
        #             edt = get_early_detection_time(
        #                 self._dag.nodes[head.node_i]['jobs'][next_job_i])
        #             next_job_i += 1
        #     except IndexError:
        #         return True

        if self._current_time > edt:
            return False
        else:
            return True

    def _pop_highest_priority_job(
        self,
        ready_jobs: List[Job]
    ) -> Job:
        if self._algorithm == 'EDF':
            # sort by implicit deadline
            ready_jobs.sort(
                key=lambda x: (self._get_containing_sub_dag(x).period
                               * (x.job_i+1))
            )
        elif self._algorithm == 'LLF':
            ready_jobs.sort(key=lambda x: x.laxity)

        return ready_jobs.pop(0)

    def _trigger_succs(
        self,
        now_finish_jobs: List[Job]
    ) -> None:
        for job in now_finish_jobs:
            if succs := self._dag.get_succ_tri(job.node_i):
                for succ_i in succs:
                    self._dag.nodes[succ_i]['jobs'][0].tri_time = \
                        (self._current_time +
                         self._dag.edges[job.node_i, succ_i]['comm'])

    def _advance_time(self) -> Optional[List[Job]]:
        self._current_time += 1
        return self._processor.process()

    def _update_ready_jobs(
        self,
        ready_jobs: List[Job]
    ) -> None:
        for node_i in self._dag.nodes:
            if (self._dag.nodes[node_i]['jobs']
                and self._dag.nodes[node_i]['jobs'][0].tri_time
                    == self._current_time):
                ready_jobs.append(self._dag.nodes[node_i]['jobs'].pop(0))

    @staticmethod
    def _validate(
        algorithm: str
    ) -> None:
        if algorithm not in Scheduler._supported_algorithm:
            raise NotImplementedError(
                f'{Scheduler._supported_algorithm} are allowed.')
