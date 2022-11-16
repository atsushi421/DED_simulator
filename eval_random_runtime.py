import argparse
import os
import glob
import yaml
import time

from memory_profiler import memory_usage

from src.random_dag_fomatter import RandomDAGFormatter
from src.dag import DAG
from src.dag_divider import DAGDivider


def option_parser():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "-g", "--dag_dir",
        required=True,
        type=str
    )
    arg_parser.add_argument(
        "-d", "--dest_dir",
        default=f"{os.path.dirname(__file__)}./results",
        type=str
    )
    arg_parser.add_argument(
        "-m", "--metrics",
        required=False,
        type=str,
        choices=['run_time', 'memory_usage']
    )
    args = arg_parser.parse_args()

    return (
        args.dag_dir,
        args.dest_dir,
        args.metrics
    )


def export_alg12_log(
    dag: DAG,
    metrics: str,
    measured_value: float,
    result_dir_path: str,
    dag_id: int
) -> None:
    log = {
        'Number of nodes': dag.number_of_nodes(),
        'Number of edges': dag.number_of_edges(),
        'Number of timer driven_nodes': len(dag.timer_nodes),
        'Number of join nodes': len([ni for ni in dag.nodes if dag.nodes[ni]['is_join']])
    }
    if metrics == 'run_time':
        log['Run time [ms]'] = measured_value * 10**(3)
    else:
        log['Maximum memory usage [MiB]'] = measured_value

    with open(f'{result_dir_path}/{metrics}/alg12/{dag_id}.yaml', 'w') as f:
        yaml.dump(log, f)


if __name__ == "__main__":
    dag_dir, dest_dir, metrics = option_parser()
    dag_paths = glob.glob(f"{dag_dir}/**/*.dot", recursive=True)
    for i, dag_path in enumerate(dag_paths):
        dag = RandomDAGFormatter.read_dot(dag_path)
        RandomDAGFormatter.format(dag)
        dag.initialize(e2e_deadline_tightness=1.5)

        # Algorithm 1 and 2
        if metrics == 'run_time':
            start_time = time.perf_counter()
            dag.sub_dags = DAGDivider.divide(dag)
            run_time = time.perf_counter() - start_time
            export_alg12_log(dag, metrics, run_time, dest_dir, i)
        else:
            max_memory_usage = memory_usage(
                (DAGDivider.divide, (dag,), {}),
                interval=0.01,
                include_children=True,
                max_usage=True
            )
            export_alg12_log(dag, metrics, max_memory_usage, dest_dir, i)

        # Algorithm 3
        # TODO
