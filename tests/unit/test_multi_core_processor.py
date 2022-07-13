import pytest
from src.exceptions import InvalidPreemptionError
from src.job_generator import Job
from src.multi_core_processor import Core, MultiCoreProcessor


class TestCore:

    def test_allocate(self, mocker):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'exec', 10)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'exec', 20)

        core = Core(0)
        core.allocate(job_mock0)
        assert core.remain == 10

        with pytest.raises(InvalidPreemptionError) as e:
            core.allocate(job_mock1)
        assert len(str(e.value)) >= 1

    def test_process(self, mocker):
        job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock, 'exec', 5)

        core = Core(0)
        core.allocate(job_mock)
        for _ in range(5):
            finish_job = core.process()

        assert not core.proc_job
        assert core.remain == 0
        assert finish_job.exec == job_mock.exec


class TestMultiCoreProcessor:

    def test_get_idle_core(self, mocker):
        processor = MultiCoreProcessor(4)
        idle_core = processor.get_idle_core()
        assert idle_core == processor.cores[0]

        job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock, 'exec', 10)
        idle_core.allocate(job_mock)
        new_idle_core = processor.get_idle_core()
        assert new_idle_core == processor.cores[1]

    def test_process_01(self, mocker):
        job_mock = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock, 'exec', 10)

        processor = MultiCoreProcessor(4)
        processor.cores[0].allocate(job_mock)
        for _ in range(10):
            finish_jobs = processor.process()
        assert not processor.cores[0].proc_job
        assert processor.cores[0].remain == 0
        assert finish_jobs[0].exec == job_mock.exec

    def test_process_02(self, mocker):
        job_mock0 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock0, 'exec', 10)
        job_mock1 = mocker.Mock(spec=Job)
        mocker.patch.object(job_mock1, 'exec', 10)

        processor = MultiCoreProcessor(4)
        processor.cores[0].allocate(job_mock0)
        processor.cores[1].allocate(job_mock1)
        for _ in range(10):
            finish_jobs = processor.process()
        assert not processor.cores[0].proc_job
        assert processor.cores[0].remain == 0
        assert not processor.cores[1].proc_job
        assert processor.cores[1].remain == 0
        assert len(finish_jobs) == 2
        assert finish_jobs[0].exec == job_mock0.exec
        assert finish_jobs[1].exec == job_mock1.exec
