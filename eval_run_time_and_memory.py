import argparse
import os
import glob
import yaml
import time
from tqdm import tqdm

from memory_profiler import memory_usage

from src.random_dag_fomatter import RandomDAGFormatter
from src.dag import DAG
from src.dag_divider import DAGDivider
from src.jld_analyzer import JLDAnalyzer
from src.job_generator import JobGenerator


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


class LogHelper:

    def __init__(self, dag: DAG) -> None:
        self._join_tail_dict = {}
        joins = [ni for ni in dag.nodes if dag.nodes[ni]['is_join']]
        for ji in joins:
            self._join_tail_dict[str(ji)] = dag.pred[ji]

        if dag.nodes[0].get('num_trigger') is not None:
            self._num_jobs_dict = {}
            for ni in dag.nodes:
                self._num_jobs_dict[str(ni)] = dag.nodes[ni]['num_trigger']

    def get_num_tails(self) -> int:
        num_tails = 0
        for tails in self._join_tail_dict.values():
            num_tails += len(tails)

        return num_tails

    def get_num_node_pair_tail_join(self) -> int:
        return self.get_num_tails()  # because tail nodes have only one succ

    def get_num_jobs(self) -> int:
        sum = 0
        for num_trigger in self._num_jobs_dict.values():
            sum += num_trigger

        return sum

    def get_num_job_pair_tail_join(self) -> int:
        num_job_pair_tail_join = 0
        for ji, tails in self._join_tail_dict.items():
            num_join_jobs = self._num_jobs_dict[ji]
            for ti in tails:
                num_tail_jobs = self._num_jobs_dict[str(ti)]
                num_job_pair_tail_join += num_join_jobs * num_tail_jobs

        return num_job_pair_tail_join


def export_alg12_log(
    dag: DAG,
    metrics: str,
    measured_value: float,
    result_dir_path: str,
    dag_id: int
) -> None:
    hl = LogHelper(dag)
    log = {
        'Number of nodes': dag.number_of_nodes(),
        'Number of edges': dag.number_of_edges(),
        'Number of timer driven nodes': len(dag.timer_nodes),
        'Number of join nodes': len([ni for ni in dag.nodes if dag.nodes[ni]['is_join']]),
        'Number of tail nodes': hl.get_num_tails(),
        'Number of tail and join node pairs': hl.get_num_node_pair_tail_join()
    }
    if metrics == 'run_time':
        log['Run time [ms]'] = measured_value * 10**(3)
    else:
        log['Maximum memory usage [MiB]'] = measured_value

    with open(f'{result_dir_path}/{metrics}/alg12/{dag_id}.yaml', 'w') as f:
        yaml.dump(log, f)


def export_alg3_log(
    dag: DAG,
    metrics: str,
    measured_value: float,
    result_dir_path: str,
    dag_id: int
) -> None:
    hl = LogHelper(dag)
    log = {
        'Number of nodes': dag.number_of_nodes(),
        'Number of edges': dag.number_of_edges(),
        'Number of timer driven nodes': len(dag.timer_nodes),
        'Number of join nodes': len([ni for ni in dag.nodes if dag.nodes[ni]['is_join']]),
        'Number of tail nodes': hl.get_num_tails(),
        'Number of sub DAGs': len(dag.sub_dags),
        'Number of tail and join node pairs': hl.get_num_node_pair_tail_join(),
        'Number of pairs of tail node jobs and join node jobs': hl.get_num_job_pair_tail_join(),
        'Number of jobs': hl.get_num_jobs(),
        'HP': dag.hp
    }
    if metrics == 'run_time':
        log['Run time [ms]'] = measured_value * 10**(3)
    else:
        log['Maximum memory usage [MiB]'] = measured_value

    with open(f'{result_dir_path}/{metrics}/alg3/{dag_id}.yaml', 'w') as f:
        yaml.dump(log, f)


if __name__ == "__main__":
    dag_dir, dest_dir, metrics = option_parser()
    dag_paths = glob.glob(f"{dag_dir}/**/*.dot", recursive=True)
    for i, dag_path in enumerate(tqdm(dag_paths)):
        dag = RandomDAGFormatter.read_dot(dag_path)
        RandomDAGFormatter.format(dag, i)
        dag.initialize(e2e_deadline_tightness=1.5)

        # Algorithm 1 and 2
        if metrics == 'run_time':
            start_time = time.perf_counter()
            dag.sub_dags = DAGDivider.divide(dag)
            run_time = time.perf_counter() - start_time
            export_alg12_log(dag, metrics, run_time, dest_dir, i)
        else:
            max_memory_usage, dag.sub_dags = memory_usage(
                (DAGDivider.divide, (dag,), {}),
                interval=0.01,
                include_children=True,
                max_usage=True,
                retval=True
            )
            export_alg12_log(dag, metrics, max_memory_usage, dest_dir, i)

        # middle process
        dag.set_num_trigger()
        JobGenerator.generate(dag)

        # Algorithm 3
        if metrics == 'run_time':
            start_time = time.perf_counter()
            dag.jld = JLDAnalyzer.analyze(dag, method='proposed', alpha=2.0)
            run_time = time.perf_counter() - start_time
            dag.reflect_jobs_in_dag()
            export_alg3_log(dag, metrics, run_time, dest_dir, i)
        else:
            max_memory_usage, dag.jld = memory_usage(
                (JLDAnalyzer.analyze, (dag, 'proposed', 2.0), {}),
                interval=0.01,
                include_children=True,
                max_usage=True,
                retval=True
            )
            dag.reflect_jobs_in_dag()
            export_alg3_log(dag, metrics, max_memory_usage, dest_dir, i)
