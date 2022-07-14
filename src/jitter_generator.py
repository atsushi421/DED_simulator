import yaml

from src.dag import DAG


class JitterGenerator:

    @staticmethod
    def generate_exec_jitter(
        jitter_src_path: str,
        dag: DAG
    ) -> None:
        with open(jitter_src_path, "r") as f:
            jitter_src = yaml.safe_load(f)
        node_name_dict = jitter_src['node_name_dict']

        for node_name, jitter_list in jitter_src['jitter'].items():
            for job in dag.nodes[node_name_dict[node_name]]['jobs']:
                job.exec = jitter_list[job.job_i % len(jitter_list)]
