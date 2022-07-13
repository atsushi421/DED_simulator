

import copy
from typing import List, Optional

from src.exceptions import InvalidPreemptionError
from src.job_generator import Job


class Core:

    def __init__(
        self
    ) -> None:
        self.proc_job: Optional[Job] = None
        self.remain = 0

    def process(
        self
    ) -> Optional[Job]:
        if self.proc_job:
            self.remain -= 1
            if self.remain == 0:
                finish_job = copy.deepcopy(self.proc_job)
                self.proc_job = None
                return finish_job

        return None

    def allocate(
        self,
        job: Job
    ) -> None:
        if self.proc_job:
            raise InvalidPreemptionError('Preemption not allowed.')

        self.proc_job = job
        self.remain = job.exec


class MultiCoreProcessor:

    def __init__(
        self,
        num_cores: int
    ) -> None:
        self.cores = [Core() for _ in range(num_cores)]

    def process(
        self
    ) -> Optional[List[Job]]:
        finish_jobs: List[Job] = []
        for core in self.cores:
            if finish_job := core.process():
                finish_jobs.append(finish_job)

        return finish_jobs

    def get_idle_core(
        self
    ) -> Optional[Core]:
        for core in self.cores:
            if not core.proc_job:
                return core

        return None
