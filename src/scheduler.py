import copy
import json
from typing import Dict, List

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
        use_sched_logger: bool
    ) -> None:
        self._validate(algorithm)

        # Initialize variables
        self._algorithm = algorithm
        self._dag = copy.deepcopy(dag)
        self._processor = copy.deepcopy(processor)
        self._alpha = alpha
        self._ready_jobs: List[Job] = []
        self._finish_jobs: List[Job] = []
        self._dm_flag = False
        self._current_time = 0
        self._use_sched_logger = use_sched_logger
        self._logger = ScheduleLogger(len(self._processor.cores))

    def schedule(self) -> None:
        while self._current_time != self._dag.hp:
            if self._dm_flag:
                break

            self._update_ready_jobs()
            if not self._ready_jobs or not self._processor.get_idle_core():
                self._advance_time()
                continue

            head = self._pop_highest_priority_job()

            if not self._early_detection(head):
                self._logger.write_early_detection(self._current_time, head)

            if (head.job_i != 0 and head.is_join
                    and not self._check_dfc(head)):
                if (self._dag.exit_i in
                        set(self._get_containing_sub_dag(head).nodes)):
                    self._logger.write_deadline_miss(
                        'dfc', self._current_time, head)
                    break
                else:
                    continue

            idle_core = self._processor.get_idle_core()
            idle_core.allocate(head)
            if self._use_sched_logger:
                self._logger.write_allocate(
                    idle_core.core_i,
                    head,
                    self._current_time,
                    self._current_time + head.exec
                )

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

    def _check_dfc(
        self,
        head: Job
    ) -> bool:
        def get_timestamp(tail_job: Job) -> int:
            sub_dag = self._get_containing_sub_dag(tail_job)
            for finish_job in reversed(self._finish_jobs):
                if (finish_job.node_i == sub_dag.head
                        and finish_job.job_i == tail_job.job_i):
                    timestamp = finish_job.tri_time

            return timestamp

        for pred_i in self._dag.pred[head.node_i]:
            for finish_job in reversed(self._finish_jobs):
                if finish_job.node_i == pred_i:
                    dfc = int(
                        self._alpha
                        * self._get_containing_sub_dag(finish_job).period
                    )
                    if self._current_time - get_timestamp(finish_job) > dfc:
                        return False
                    else:
                        break

        return True

    def _early_detection(
        self,
        head: Job
    ) -> bool:
        laxity = head.laxity
        if laxity > NO_LAXITY_CRITERIA:
            next_job_i = head.job_i + 1
            try:
                while laxity > NO_LAXITY_CRITERIA:
                    laxity = \
                        self._dag.nodes[head.node_i]['jobs'][next_job_i].laxity
                    next_job_i += 1
            except IndexError:
                return True

        if self._current_time > laxity:
            return False
        else:
            return True

    def _pop_highest_priority_job(self) -> Job:
        if self._algorithm == 'EDF':
            # sort by implicit deadline
            self._ready_jobs.sort(
                key=lambda x: (self._get_containing_sub_dag(x).period
                               * (x.job_i+1))
            )
        elif self._algorithm == 'LLF':
            self._ready_jobs.sort(key=lambda x: x.laxity)

        return self._ready_jobs.pop(0)

    def _advance_time(self) -> None:
        self._current_time += 1
        if fin_jobs := self._processor.process():
            self._finish_jobs += fin_jobs
            for fin_job in fin_jobs:
                # Trigger successor event-driven nodes
                if succs := self._dag.get_succ_tri(fin_job.node_i):
                    for succ_i in succs:
                        self._dag.nodes[succ_i]['jobs'][0].tri_time = \
                            (self._current_time +
                             self._dag.edges[fin_job.node_i, succ_i]['comm'])

                # Check deadline miss
                elif (fin_job.node_i == self._dag.exit_i and
                      self._current_time > fin_job.deadline):
                    self._dm_flag = True
                    self._logger.write_deadline_miss(
                        'e2e deadline', self._current_time, fin_job)

    def _update_ready_jobs(
        self
    ) -> None:
        for node_i in self._dag.nodes:
            if (self._dag.nodes[node_i]['jobs']
                and self._dag.nodes[node_i]['jobs'][0].tri_time
                    == self._current_time):
                self._ready_jobs.append(self._dag.nodes[node_i]['jobs'].pop(0))

    # @staticmethod
    # def _remove_unnecessary_jobs(
    #     dag: DAG
    # ) -> None:
    #     for node_i in dag.nodes:
    #         dag.nodes[node_i]['jobs'] = \
    #             dag.nodes[node_i]['jobs'][:dag.nodes[node_i]['num_trigger']]

    @staticmethod
    def _validate(
        algorithm: str
    ) -> None:
        if algorithm not in Scheduler._supported_algorithm:
            raise NotImplementedError(
                f'{Scheduler._supported_algorithm} are allowed.')
