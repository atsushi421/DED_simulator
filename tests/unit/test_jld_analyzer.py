import pytest
from src.jld_analyzer import JLDAnalyzer


class TestJLDAnalyzer:

    def test_validate(self):
        JLDAnalyzer._validate('proposed')
        JLDAnalyzer._validate('Igarashi')
        JLDAnalyzer._validate('Saidi')

        with pytest.raises(NotImplementedError) as e:
            JLDAnalyzer._validate('method')
        assert len(str(e.value)) >= 1
