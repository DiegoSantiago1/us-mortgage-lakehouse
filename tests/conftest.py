"""Uma SparkSession para a suíte inteira (subir o Spark custa segundos)."""

import pytest

from hmda.spark import criar_sessao


@pytest.fixture(scope="session")
def spark():
    sessao = criar_sessao("testes", driver_memory="1g", nucleos="2", particoes_shuffle=2)
    yield sessao
    sessao.stop()
