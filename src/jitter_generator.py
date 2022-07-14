from typing import Dict

import yaml

from src.dag import DAG


class JitterGenerator:

    @staticmethod
    def generate_exec_jitter(
        jitter_src_path: str,
        node_name_dict: Dict[str, int],
        dag: DAG
    ) -> None:
        with open(jitter_src_path, "r") as f:
            exec_jitter = yaml.safe_load(f)

        for node_name, jitter_list in exec_jitter.items():
            for job in dag.nodes[node_name_dict[node_name]]['jobs']:
                job.exec = jitter_list[job.job_i % len(jitter_list)]
