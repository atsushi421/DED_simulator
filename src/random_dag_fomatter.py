import networkx as nx
import random

from src.dag import DAG


class RandomDAGFormatter:

    @staticmethod
    def format(dag: DAG) -> None:
        # Convert exit node to timer-driven node
        period_choice = [10, 20, 30, 40, 50, 60, 80, 100, 120]
        exit_i = [v for v, d in dag.out_degree() if d == 0][0]
        dag.nodes[exit_i]['period'] = random.choice(period_choice)

        # Find timer-driven nodes & is_update=True, is_join=True
        for node_i in dag.nodes:
            if dag.nodes[node_i].get('period') is None:
                continue
            # timer-driven node
            preds = dag.pred[node_i]
            if preds:
                dag.nodes[node_i]['is_join'] = True
            for pred_i in preds:
                dag.edges[pred_i, node_i]['is_update'] = True

    @staticmethod
    def read_dot(path: str) -> DAG:
        tmp_dag = nx.drawing.nx_pydot.read_dot(path)
        tmp_dag = nx.DiGraph(tmp_dag)
        tmp_dag.remove_node('\\n')
        return RandomDAGFormatter._get_dag_from_tmp_dag(tmp_dag)

    @staticmethod
    def _get_dag_from_tmp_dag(
        tmp_dag: nx.DiGraph
    ) -> DAG:
        dag = DAG()
        for node_i in tmp_dag.nodes:
            dag.add_node(int(node_i), is_join=False)
            node_properties = tmp_dag.nodes[node_i].keys()
            for np in node_properties:
                if np == 'Execution_time':
                    dag.nodes[int(node_i)]['exec'] = \
                        int(tmp_dag.nodes[node_i]['Execution_time'])
                elif np == 'Period':
                    dag.nodes[int(node_i)]['period'] = \
                        int(tmp_dag.nodes[node_i]['Period'])
                else:
                    dag.nodes[int(node_i)][np] = int(tmp_dag.nodes[node_i][np])

        for s, t in tmp_dag.edges:
            dag.add_edge(int(s), int(t), is_update=False)
            dag.edges[int(s), int(t)]['comm'] = 1

        return dag
