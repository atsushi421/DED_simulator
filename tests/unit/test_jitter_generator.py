from src.jitter_generator import JitterGenerator


class TestJitterGenerator:

    def test_set_wcet(self, EG, mocker):
        mocker.patch('yaml.safe_load',
                     return_value={
                         'node_name_dict': {'node0': 0, 'node1': 1},
                         'jitter': {
                             'node0': [1, 2, 3, 4, 5],
                             'node1': [3, 4, 3, 4, 7]
                         }
                     })
        jitter_gen = JitterGenerator(f'{__file__}', "1.0")
        jitter_gen.set_wcet(EG)
        assert EG.nodes[0]['exec'] == 5
        assert EG.nodes[1]['exec'] == 7

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
