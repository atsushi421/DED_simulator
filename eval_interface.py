import argparse
import os

from src.dag_divider import DAGDivider
from src.dag_reader import DAGReader
from src.jitter_generator import JitterGenerator
from src.jld_analyzer import JLDAnalyzer
from src.job_generator import JobGenerator
from src.laxity_calculator import LaxityCalculator
from src.multi_core_processor import MultiCoreProcessor
from src.scheduler import Scheduler


def option_parser():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "-g", "--dag_path",
        required=True,
        type=str
    )
    arg_parser.add_argument(
        "-d", "--dest_dir",
        default=f"{os.path.dirname(__file__)}./results",
        type=str
    )
    arg_parser.add_argument(
        "-t", "--e2e_deadline_tightness",
        required=False,
        default=1.2,
        type=float
    )
    arg_parser.add_argument(
        "-m", "--analyze_method",
        required=True,
        type=str,
        choices=['proposed', 'Igarashi', 'Saidi']
    )
    arg_parser.add_argument(
        "--alpha",
        required=True,
        type=float
    )
    arg_parser.add_argument(
        "-j", "--jitter",
        nargs='+',
        required=False,
        help=('arg 1: jitter source path, '
              'arg 2: factor, '
              'arg 3: random seed (Optional)')
    )
    arg_parser.add_argument(
        "-c", "--num_cores",
        required=True,
        type=int
    )
    arg_parser.add_argument(
        "-a", "--sched_algorithm",
        required=True,
        type=str,
        choices=['EDF', 'LLF']
    )
    arg_parser.add_argument(
        "-l", "--write_sched_log",
        required=False,
        action="store_true",
    )
    arg_parser.add_argument(
        "-u", "--calc_utilization",
        required=False,
        action="store_true",
    )
    args = arg_parser.parse_args()

    return (
        args.dag_path,
        args.dest_dir,
        args.e2e_deadline_tightness,
        args.analyze_method,
        args.alpha,
        args.jitter,
        args.num_cores,
        args.sched_algorithm,
        args.write_sched_log,
        args.calc_utilization
    )


if __name__ == "__main__":
    (dag_path, dest_dir, e2e_deadline_tightness,
     analyze_method, alpha, jitter, num_cores,
     sched_algorithm, write_sched_log, calc_utilization) = option_parser()

    dag = DAGReader._read_dot(dag_path)
    dag.initialize(e2e_deadline_tightness)
    dag.sub_dags = DAGDivider.divide(dag)
    dag.set_num_trigger()
    if jitter:
        jitter_generator = JitterGenerator(*jitter)
        jitter_generator.set_wcet(dag)
    JobGenerator.generate(dag)

    dag.jld = JLDAnalyzer.analyze(dag, analyze_method, alpha)
    dag.reflect_jobs_in_dag()
    LaxityCalculator.calculate(dag)

    if jitter:
        jitter_generator.generate_exec_jitter(dag)
    if calc_utilization:
        total_utilization = dag.get_total_utilization()

    processor = MultiCoreProcessor(num_cores)
    scheduler = Scheduler(sched_algorithm, dag,
                          processor, alpha, write_sched_log)
    scheduler.schedule()

    file_name = (
        f'{os.path.splitext(os.path.basename(dag_path))[0]}_'
        f'{analyze_method}_'
        f'alpha={alpha}_'
        f'numCores={num_cores}_'
        f'{sched_algorithm}_'
    )
    if jitter:
        file_name += f'jitterFactor={jitter[1]}_'
    if calc_utilization:
        file_name += f'totalUtilization={total_utilization/num_cores}'
    logger = scheduler.create_logger()
    logger.dump_dm_log(f'{dest_dir}/{file_name}.yaml')
    if write_sched_log:
        logger.dump_sched_log(f'{dest_dir}/{file_name}.json')
