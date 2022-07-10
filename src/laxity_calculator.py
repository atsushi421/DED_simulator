

import sys
from typing import List

from src.dag import DAG
from src.job_generator import Job


class LaxityCalculator:

    @staticmethod
    def calculate(
        dag: DAG
    ) -> None:
        def recur_calc_laxity(job: Job) -> int:
            if job.deadline:
                job.laxity = job.deadline - job.exec
            elif succ_jobs := dag.jld.get_succ_jobs(job):
                values: List[int] = []
                for succ_job in succ_jobs:
                    values.append(
                        recur_calc_laxity(succ_job)
                        - dag.edges[job.node_i, succ_job.node_i]['comm']
                    )
                job.laxity = min(values) - job.exec
            else:
                job.laxity = sys.maxsize

            return job.laxity

        for sub_dag in dag.sub_dags:
            for job_i in range(dag.nodes[sub_dag.head]['num_trigger']):
                recur_calc_laxity(dag.nodes[sub_dag.head]['jobs'][job_i])
