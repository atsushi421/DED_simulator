from typing import Dict, List, Optional, Set

from src.dag import DAG
from src.sub_dag import SubDAG


class Job:
    def __init__(
        self,
        node_i: int,
        job_i: int,
        exec: int,
        is_join: bool,
        rst: int,
        rft: int,
        tri_time: Optional[int],
        deadline: Optional[int] = None
    ) -> None:
        self._node_i = node_i
        self._job_i = job_i
        self._exec = exec
        self._is_join = is_join
        self._rst = rst
        self._rft = rft
        self._tri_time = tri_time
        self._deadline = deadline

    @property
    def node_i(self):
        return self._node_i

    @property
    def job_i(self):
        return self._job_i

    @property
    def is_join(self):
        return self._is_join

    @property
    def rst(self):
        return self._rst

    @property
    def rft(self):
        return self._rft

    @property
    def tri_time(self):
        return self._tri_time

    @tri_time.setter
    def tri_time(
        self,
        tri_time: int
    ):
        self._tri_time = tri_time

    @property
    def deadline(self):
        return self._deadline

    @property
    def exec(self):
        return self._exec

    @exec.setter
    def exec(
        self,
        exec: int
    ):
        self._exec = exec

    @property
    def laxity(self):
        return self._laxity

    @laxity.setter
    def laxity(
        self,
        laxity: int
    ):
        self._laxity = laxity


class JobGenerator:

    @staticmethod
    def generate(
        dag: DAG
    ) -> None:
        for sub_dag in dag.sub_dags:
            # Set num_trigger
            sub_dag_num_trigger = int(dag.hp / sub_dag.period)
            for node_i in sub_dag.nodes:
                dag.nodes[node_i]['num_trigger'] = sub_dag_num_trigger

            # Initialize variables
            ready_nodes: List[int] = [sub_dag.head]
            finish_nodes: Set[int] = set()

            # Generate jobs
            while ready_nodes:
                node_i = ready_nodes.pop(0)
                jobs: List[Job] = []
                for i in range(sub_dag_num_trigger*2):  # HACK
                    jobs.append(Job(
                        **JobGenerator._get_job_args(sub_dag, node_i, i)))

                sub_dag.nodes[node_i]['jobs'] = jobs
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
                    'exec': sub_dag.nodes[node_i]['exec'],
                    'is_join': sub_dag.nodes[node_i]['is_join']}

        if node_i == sub_dag.head:
            job_args['rst'] = sub_dag.period * job_i
            job_args['tri_time'] = job_args['rst']
        else:
            rst_list: List[int] = []
            for pred_i in sub_dag.pred[node_i]:
                rst_list.append(sub_dag.nodes[pred_i]['jobs'][job_i].rft
                                + sub_dag.edges[pred_i, node_i]['comm'])
            job_args['rst'] = max(rst_list)
            job_args['tri_time'] = None

        job_args['rft'] = (job_args['rst'] +
                           sub_dag.nodes[node_i]['exec'])

        if deadline := sub_dag.nodes[node_i].get('deadline'):
            job_args['deadline'] = (deadline + job_i * sub_dag.period)

        return job_args

    @staticmethod
    def _get_ready_nodes(
        sub_dag: SubDAG,
        finish_nodes: Set[int],
        finish_node_i: int
    ) -> List[int]:
        ready_nodes = []
        for succ_i in sub_dag.succ[finish_node_i]:
            if set(sub_dag.pred[succ_i]) <= finish_nodes:
                ready_nodes.append(succ_i)

        return ready_nodes
