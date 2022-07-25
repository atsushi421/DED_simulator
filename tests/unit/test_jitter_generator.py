from src.jitter_generator import JitterGenerator


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

    def test_set_wcet(self, EG_divided, mocker):
        mocker.patch('yaml.safe_load',
                     return_value={
                         'node_name_dict': {'node0': 0, 'node1': 1},
                         'jitter': {
                             'node0': [1, 2, 3, 4, 5],
                             'node1': [3, 4, 3, 4, 7]
                         }
                     })
        jitter_gen = JitterGenerator(f'{__file__}', "1.0")
        jitter_gen.set_wcet(EG_divided)
        assert EG_divided.nodes[0]['exec'] == 5
        assert EG_divided.nodes[1]['exec'] == 4

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
