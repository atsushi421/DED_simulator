from src.jitter_generator import JitterGenerator
import pandas as pd


class TestJitterGenerator:

    def test_randomly_increase_exec(self, mocker):
        mocker.patch('yaml.safe_load',
                     return_value={
                         'node_name_dict': {'node0': 0, 'node1': 1},
                         'jitter': {
                             'node0': [1, 2, 3, 4, 5],
                             'node1': [3, 4, 3, 4, 7]
                         }
                     })
        jitter_gen1 = JitterGenerator(f'{__file__}', "1.5", 0)
        jitter_gen2 = JitterGenerator(f'{__file__}', "1.5", 0)

        for one, two in zip(jitter_gen1._jitter_dict['node0'],
                            jitter_gen2._jitter_dict['node0']):
            assert one == two
        for one, two in zip(jitter_gen1._jitter_dict['node1'],
                            jitter_gen2._jitter_dict['node1']):
            assert one == two

    def test_set_exec_no_percentile(self, EG_divided, mocker):
        mocker.patch('yaml.safe_load',
                     return_value={
                         'node_name_dict': {'node0': 0, 'node1': 1},
                         'jitter': {
                             'node0': [1, 2, 3, 4, 5],
                             'node1': [3, 4, 3, 4, 7]
                         }
                     })
        jitter_gen = JitterGenerator(f'{__file__}', "1.0")
        jitter_gen.set_exec(EG_divided)
        assert EG_divided.nodes[0]['exec'] == 5
        assert EG_divided.nodes[1]['exec'] == 4

    def test_set_exec_percentile(self, EG_divided, mocker):
        node0_exec_list = [1, 2, 3, 4, 5]
        node1_exec_list = [3, 4, 3, 4, 7]
        mocker.patch('yaml.safe_load',
                     return_value={
                         'node_name_dict': {'node0': 0, 'node1': 1},
                         'jitter': {
                             'node0': node0_exec_list,
                             'node1': node1_exec_list
                         }
                     })
        jitter_gen = JitterGenerator(f'{__file__}', "1.0")

        percentile = 0.9
        jitter_gen.set_exec(EG_divided, percentile)
        assert EG_divided.nodes[0]['exec'] == int(pd.Series(node0_exec_list).quantile(percentile))
        assert EG_divided.nodes[1]['exec'] == int(pd.Series(node1_exec_list[:3]).quantile(percentile))

    def test_generate_exec_jitter(self, EG_calculated, mocker):
        mocker.patch('yaml.safe_load',
                     return_value={
                         'node_name_dict': {'node0': 0, 'node1': 1},
                         'jitter': {
                             'node0': [1, 2, 3, 4, 5],
                             'node1': [3, 4, 3, 4, 7]
                         }
                     })
        jitter_gen = JitterGenerator(f'{__file__}', "1.0")
        jitter_gen.generate_exec_jitter(EG_calculated)

        for job in EG_calculated.nodes[0]['jobs']:
            assert job.exec in [1, 2, 3, 4, 5]
        for job in EG_calculated.nodes[1]['jobs']:
            assert job.exec in [3, 4, 3, 4, 7]
