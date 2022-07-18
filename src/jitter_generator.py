import yaml

from src.dag import DAG


class JitterGenerator:
    def __init__(
        self,
        jitter_src_path: str,
        factor: str
    ) -> None:
        with open(jitter_src_path, "r") as f:
            jitter_src = yaml.safe_load(f)
        self._node_name_dict = jitter_src['node_name_dict']
        self._jitter_dict = jitter_src['jitter']
        self._factor = float(factor)

    def set_wcet(
        self,
        dag: DAG
    ) -> None:
        for node_name, jitter_list in self._jitter_dict.items():
            num_trigger = \
                dag.nodes[self._node_name_dict[node_name]]['num_trigger']
            wcet = max(jitter_list[:num_trigger])
            dag.nodes[self._node_name_dict[node_name]]['exec'] = wcet

    def generate_exec_jitter(
        self,
        dag: DAG
    ) -> None:
        for node_name, jitter_list in self._jitter_dict.items():
            for job in dag.nodes[self._node_name_dict[node_name]]['jobs']:
                job.exec = (jitter_list[job.job_i % len(jitter_list)]
                            * self._factor)
