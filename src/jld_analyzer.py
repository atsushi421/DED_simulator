from src.dag import DAG
from src.sub_dag import SubDAG


class JLDAnalyzer:
    _supported_method = ['proposed', 'Igarashi', 'Saidi']

    @staticmethod
    def analyze(
        dag: DAG,
        method: str,
        alpha: float = None,
    ):
        JLDAnalyzer._validate(method)
        pass

    # @staticmethod
    # def _calc_st_ft(
    #     dag: DAG
    # ) -> None:
    #     def set_jobs(sub_dag: SubDAG, node_i: int) -> None:
    #         preds = sub_dag.dag.pred[node_i]
    #         if (not preds or [pred_i for pred_i in preds
    #                           if sub_dag.dag.nodes[pred_i].get('jobs')]):
    #             sub_dag.dag.nodes[node_i]['jobs'] = []
    #             for _ in range(sub_dag.num_trigger*2):
    #                 pass  # TODO

    #     for sub_dag in dag.sub_dags:
    #         sub_dag.num_trigger = int(dag.hp / sub_dag.period)

    #         num_finish_nodes = 0
    #         while num_finish_nodes == sub_dag.dag.number_of_nodes():
    #             pass  # TODO

    @ staticmethod
    def _validate(
        method: str
    ) -> None:
        if method not in JLDAnalyzer._supported_method:
            raise NotImplementedError(
                f'{JLDAnalyzer._supported_method} are allowed.')
