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
        default=f"{os.path.dirname(__file__)}./result",
        type=str
    )
    arg_parser.add_argument(
        "-m", "--analyze_method",
        required=True,
        type=str,
        help="['proposed', 'Igarashi', 'Saidi'] are allowed."
    )
    arg_parser.add_argument(
        "--alpha",
        required=True,
        type=float
    )
    arg_parser.add_argument(
        "-j", "--jitter_src_path",
        required=False,
        type=str
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
        help="['EDF', 'LLF'] are allowed."
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
        args.analyze_method,
        args.alpha,
        args.jitter_src_path,
        args.num_cores,
        args.sched_algorithm,
        args.write_sched_log,
        args.calc_utilization
    )


if __name__ == "__main__":
    (dag_path, dest_dir, analyze_method, alpha, jitter_src_path, num_cores,
     sched_algorithm, write_sched_log, calc_utilization) = option_parser()

    dag = DAGReader._read_dot(dag_path)
    dag.sub_dags = DAGDivider.divide(dag)
    JobGenerator.generate(dag)
    if calc_utilization:
        total_utilization = dag.get_total_utilization()

    dag.jld = JLDAnalyzer.analyze(dag, analyze_method, alpha)
    dag.reflect_jobs_in_dag()
    LaxityCalculator.calculate(dag)

    if jitter_src_path:
        JitterGenerator.generate_exec_jitter(jitter_src_path, dag)
    processor = MultiCoreProcessor(num_cores)
    scheduler = Scheduler(sched_algorithm, dag,
                          processor, alpha, write_sched_log)
    scheduler.schedule()

    file_name = (
        f'{os.path.splitext(os.path.basename(dag_path))[0]}_'
        f'{analyze_method}_'
        f'alpha={alpha}_'
        f'jitter={str(isinstance(jitter_src_path, str))}_'
        f'numCores={num_cores}_'
        f'{sched_algorithm}'
    )
    if calc_utilization:
        file_name += f'totalUtilization={total_utilization/num_cores}'
    logger = scheduler.create_logger()
    logger.dump_dm_log(f'{dest_dir}/{file_name}.yaml')
    if write_sched_log:
        logger.dump_sched_log(f'{dest_dir}/{file_name}.json')
