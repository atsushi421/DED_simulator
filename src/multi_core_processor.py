

from typing import Optional

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
    ) -> None:
        if self.proc_job:
            self.remain -= 1
            if self.remain == 0:
                self.proc_job = None

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
    ) -> None:
        for core in self.cores:
            core.process()

    def get_idle_core(
        self
    ) -> Optional[Core]:
        for core in self.cores:
            if not core.proc_job:
                return core

        return None
