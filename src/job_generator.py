from typing import Dict, List, Set

from src.dag import DAG
from src.sub_dag import SubDAG


class Job:
    def __init__(
        self,
        node_i: int,
        job_i: int,
        exec: int,
        rst: int,
        rft: int
    ) -> None:
        self.node_i = node_i
        self.job_i = job_i
        self.exec = exec
        self.rst = rst
        self.rft = rft


class JobGenerator:

    @staticmethod
    def generate(
        dag: DAG
    ) -> None:
        for sub_dag in dag.sub_dags:
            sub_dag.num_trigger = int(dag.hp / sub_dag.period)

            ready_nodes: List[int] = [sub_dag.head]
            finish_nodes: Set[int] = set()

            while ready_nodes:
                node_i = ready_nodes.pop(0)
                jobs: List[Job] = []
                for i in range(sub_dag.num_trigger*2):  # HACK
                    jobs.append(Job(
                        **JobGenerator._get_job_args(sub_dag, node_i, i)))

                sub_dag.dag.nodes[node_i]['jobs'] = jobs
                finish_nodes.add(node_i)
                ready_nodes += JobGenerator._get_ready_nodes(
                    sub_dag, finish_nodes, node_i)

    @staticmethod
    def _get_job_args(
        sub_dag: SubDAG,
        node_i: int,
        job_i: int
    ) -> Dict:
        job_args = {'node_i': node_i,
                    'job_i': job_i,
                    'exec': sub_dag.dag.nodes[node_i]['exec']}

        if node_i == sub_dag.head:
            job_args['rst'] = sub_dag.period * job_i
        else:
            rst_list: List[int] = []
            for pred_i in sub_dag.dag.pred[node_i]:
                rst_list.append(sub_dag.dag.nodes[pred_i]['jobs'][job_i].rft
                                + sub_dag.dag.edges[pred_i, node_i]['comm'])
            job_args['rst'] = max(rst_list)

        job_args['rft'] = (job_args['rst'] +
                           sub_dag.dag.nodes[node_i]['exec'])

        return job_args

    @staticmethod
    def _get_ready_nodes(
        sub_dag: SubDAG,
        finish_nodes: Set[int],
        finish_node_i: int
    ) -> List[int]:
        ready_nodes = []
        for succ_i in sub_dag.dag.succ[finish_node_i]:
            if set(sub_dag.dag.pred[succ_i]) <= finish_nodes:
                ready_nodes.append(succ_i)

        return ready_nodes
