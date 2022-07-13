import copy
from typing import List

from src.dag import DAG
from src.job_generator import Job
from src.multi_core_processor import MultiCoreProcessor
from src.sub_dag import SubDAG

NO_LAXITY_CRITERIA = 9000000000000000000  # HACK


class Scheduler:
    _supported_algorithm = ['EDF', 'LLF']

    def __init__(
        self,
        algorithm: str,
        dag: DAG,
        processor: MultiCoreProcessor,
        alpha: float
    ) -> None:
        self._validate(algorithm)
        self._remove_unnecessary_jobs(dag)

        # Initialize variables
        self._algorithm = algorithm
        self._dag = copy.deepcopy(dag)
        self._processor = copy.deepcopy(processor)
        self._alpha = alpha
        self._ready_jobs: List[Job] = []
        self._finish_jobs: List[Job] = []
        self._dm_flag = False
        self._current_time = 0

    def schedule(self) -> None:
        last_job = self._get_last_job()
        while last_job not in self._finish_jobs:
            if self._dm_flag:
                break  # TODO: deadline miss

            self._update_ready_jobs()
            if not self._ready_jobs or not self._processor.get_idle_core():
                self._advance_time()
                continue

            head = self._pop_highest_priority_job()

            if not self._early_detection(head):
                break  # TODO: deadline miss

            if head.job_i != 0 and head.is_join and self._check_dfc(head):
                if (self._dag.exit_i in
                        set(self._get_containing_sub_dag(head).nodes)):
                    break  # TODO: deadline miss
                else:
                    continue

            idle_core = self._processor.get_idle_core()
            idle_core.allocate(head)

    def _get_last_job(self) -> Job:
        exit_node = self._dag.nodes[self._dag.exit_i]

        return exit_node['jobs'][exit_node['num_trigger']-1]

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
                if finish_job.node_i == sub_dag.head:
                    timestamp = finish_job.tri_time

            return timestamp

        for pred_i in self._dag.pred[head.node_i]:
            for finish_job in reversed(self._finish_jobs):
                if finish_job.node_i == pred_i:
                    dfc = (self._alpha
                           * self._get_containing_sub_dag(finish_job).period)
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
            while laxity > NO_LAXITY_CRITERIA:
                laxity = self._dag.nodes[head.node_i]['jobs'][next_job_i].laxity
                next_job_i += 1

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
                        self._dag.nodes[succ_i]['jobs'][fin_job.job_i].tri_time = \
                            (self._current_time +
                             self._dag.edges[fin_job.node_i, succ_i]['comm'])

                # Check deadline miss
                elif (fin_job.node_i == self._dag.exit_i and
                      self._current_time > fin_job.deadline):
                    self._dm_flag = True

    def _update_ready_jobs(
        self
    ) -> None:
        for node_i in self._dag.nodes:
            if (self._dag.nodes[node_i]['jobs']
                and self._dag.nodes[node_i]['jobs'][0].tri_time
                    == self._current_time):
                self._ready_jobs.append(self._dag.nodes[node_i]['jobs'].pop(0))

    @staticmethod
    def _remove_unnecessary_jobs(
        dag: DAG
    ) -> None:
        for node_i in dag.nodes:
            dag.nodes[node_i]['jobs'] = \
                dag.nodes[node_i]['jobs'][:dag.nodes[node_i]['num_trigger']]

    @staticmethod
    def _validate(
        algorithm: str
    ) -> None:
        if algorithm not in Scheduler._supported_algorithm:
            raise NotImplementedError(
                f'{Scheduler._supported_algorithm} are allowed.')
