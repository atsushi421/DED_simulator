from src.laxity_calculator import LaxityCalculator


class TestLaxityCalculator:

    def test_calculate(self, EG_analyzed):
        LaxityCalculator.calculate(EG_analyzed)

        for job in EG_analyzed.nodes[0]['jobs']:
            if job.job_i == 0:
                assert job.laxity == 90
            if job.job_i == 2:
                assert job.laxity == 140
            if job.job_i == 4:
                assert job.laxity == 190
            if job.job_i == 5:
                assert job.laxity == 240
            if job.job_i == 7:
                assert job.laxity == 290
            if job.job_i == 9:
                assert job.laxity == 340

        for job in EG_analyzed.nodes[1]['jobs']:
            if job.job_i == 0:
                assert job.laxity == 84
            if job.job_i == 1:
                assert job.laxity == 184
            if job.job_i == 2:
                assert job.laxity == 284

        for job in EG_analyzed.nodes[2]['jobs']:
            if job.job_i == 0:
                assert job.laxity == 101
            if job.job_i == 2:
                assert job.laxity == 151
            if job.job_i == 4:
                assert job.laxity == 201
            if job.job_i == 5:
                assert job.laxity == 251
            if job.job_i == 7:
                assert job.laxity == 301
            if job.job_i == 9:
                assert job.laxity == 351

        for job in EG_analyzed.nodes[3]['jobs']:
            if job.job_i == 0:
                assert job.laxity == 101
            if job.job_i == 1:
                assert job.laxity == 201
            if job.job_i == 2:
                assert job.laxity == 301

        for job in EG_analyzed.nodes[4]['jobs']:
            if job.job_i == 0:
                assert job.laxity == 79
            if job.job_i == 2:
                assert job.laxity == 129
            if job.job_i == 3:
                assert job.laxity == 129
            if job.job_i == 5:
                assert job.laxity == 179
            if job.job_i == 7:
                assert job.laxity == 229
            if job.job_i == 8:
                assert job.laxity == 229
            if job.job_i == 10:
                assert job.laxity == 279
            if job.job_i == 12:
                assert job.laxity == 329
            if job.job_i == 13:
                assert job.laxity == 329

        for job in EG_analyzed.nodes[5]['jobs']:
            if job.job_i == 0:
                assert job.laxity == 68
            if job.job_i == 1:
                assert job.laxity == 118
            if job.job_i == 2:
                assert job.laxity == 168
            if job.job_i == 3:
                assert job.laxity == 218
            if job.job_i == 4:
                assert job.laxity == 268
            if job.job_i == 5:
                assert job.laxity == 318

        for job in EG_analyzed.nodes[6]['jobs']:
            if job.job_i == 0:
                assert job.laxity == 89
            if job.job_i == 1:
                assert job.laxity == 139
            if job.job_i == 2:
                assert job.laxity == 189
            if job.job_i == 3:
                assert job.laxity == 239
            if job.job_i == 4:
                assert job.laxity == 289
            if job.job_i == 5:
                assert job.laxity == 339
