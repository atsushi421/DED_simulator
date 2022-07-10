import sys
from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np

from src.dag import DAG
from src.job_generator import Job
from src.sub_dag import SubDAG


class JLD:
    def __init__(self) -> None:
        self._succ_job: Dict[int, List[Job]] = defaultdict(list)
        self._pred_job: Dict[int, List[Job]] = defaultdict(list)

    def add_succ_job(
        self,
        job: Job,
        succ_job: Job
    ) -> None:
        self._succ_job[id(job)].append(succ_job)
        self._pred_job[id(succ_job)].append(job)

    def get_succ_jobs(
        self,
        job: Job
    ) -> Optional[List[Job]]:
        return self._succ_job[id(job)]

    def get_pred_jobs(
        self,
        job: Job
    ) -> Optional[List[Job]]:
        return self._pred_job[id(job)]


class JLDAnalyzer:
    _supported_method = ['proposed', 'Igarashi', 'Saidi']

    @staticmethod
    def analyze(
        dag: DAG,
        method: str,
        alpha: Optional[float] = None,
    ) -> JLD:
        JLDAnalyzer._validate(method)
        jld = JLD()
        JLDAnalyzer._analyze_in_sub_dag(dag, jld)

        if method == 'proposed':
            JLDAnalyzer._analyze_proposed(dag, jld, alpha)
        elif method == 'Igarashi':
            JLDAnalyzer._analyze_Igarashi(dag, jld)
        elif method == 'Saidi':
            JLDAnalyzer._analyze_Saidi(dag, jld)

        return jld

    @staticmethod
    def _analyze_proposed(
        dag: DAG,
        jld: JLD,
        alpha: Optional[float]
    ) -> None:
        pass

    @staticmethod
    def _analyze_Igarashi(
        dag: DAG,
        jld: JLD
    ) -> None:
        for sub_dag in dag.sub_dags:
            for tail_i in sub_dag.tails:
                for join_i in dag.succ[tail_i]:
                    join_sg = JLDAnalyzer._get_containing_sub_dag(
                        dag, join_i)
                    tail_period = sub_dag.period
                    join_period = join_sg.period

                    for tail_job in sub_dag.nodes[tail_i]['jobs']:
                        min_rst_diff = sys.maxsize
                        tail_st_assume_timer = tail_period * tail_job.job_i
                        for join_job in join_sg.nodes[join_i]['jobs']:
                            join_st_assume_timer = join_period * join_job.job_i
                            rst_diff = abs(tail_st_assume_timer
                                           - join_st_assume_timer)
                            if rst_diff < min_rst_diff:
                                min_rst_diff = rst_diff
                                min_rst_diff_job = join_job
                            if tail_st_assume_timer < join_st_assume_timer:
                                break

                        jld.add_succ_job(tail_job, min_rst_diff_job)

    @staticmethod
    def _analyze_Saidi(
        dag: DAG,
        jld: JLD
    ) -> None:
        for sub_dag in dag.sub_dags:
            for tail_i in sub_dag.tails:
                for join_i in dag.succ[tail_i]:
                    join_sg = JLDAnalyzer._get_containing_sub_dag(
                        dag, join_i)
                    tail_period = sub_dag.period
                    join_period = join_sg.period

                    # slow to fast
                    if tail_period > join_period:
                        for tail_job in sub_dag.nodes[tail_i]['jobs']:
                            succ_join_i = int(np.ceil(tail_job.job_i
                                                      * tail_period
                                                      / join_period))
                            jld.add_succ_job(
                                tail_job,
                                join_sg.nodes[join_i]['jobs'][succ_join_i]
                            )
                    # fast to slow
                    else:
                        for join_job in join_sg.nodes[join_i]['jobs']:
                            pred_tail_i = int(np.floor(join_job.job_i
                                                       * join_period
                                                       / tail_period))
                            jld.add_succ_job(
                                sub_dag.nodes[tail_i]['jobs'][pred_tail_i],
                                join_job
                            )

    @staticmethod
    def _get_containing_sub_dag(
        dag: DAG,
        node_i: int
    ) -> SubDAG:
        for sub_dag in dag.sub_dags:
            if node_i in set(sub_dag.nodes):
                return sub_dag

    @staticmethod
    def _analyze_in_sub_dag(
        dag: DAG,
        jld: JLD
    ) -> None:
        def recur_add_succ_jobs(
            node_i: int,
            sub_dag: SubDAG
        ) -> None:
            for succ_i in sub_dag.succ[node_i]:
                for job_i, job in enumerate(sub_dag.nodes[node_i]['jobs']):
                    jld.add_succ_job(
                        job,
                        sub_dag.nodes[succ_i]['jobs'][job_i]
                    )
                recur_add_succ_jobs(succ_i, sub_dag)

        for sub_dag in dag.sub_dags:
            recur_add_succ_jobs(sub_dag.head, sub_dag)

    @ staticmethod
    def _validate(
        method: str
    ) -> None:
        if method not in JLDAnalyzer._supported_method:
            raise NotImplementedError(
                f'{JLDAnalyzer._supported_method} are allowed.')
