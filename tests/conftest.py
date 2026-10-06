"""Uma SparkSession para a suíte inteira (subir o Spark custa segundos)."""

import pytest

from hmda.spark import criar_sessao


@pytest.fixture(scope="session")
def spark():
    # Dados de teste são minúsculos: menos partições e sem UI deixam o Delta bem mais rápido.
    rapido = {
        "spark.ui.enabled": "false",
        "spark.default.parallelism": "2",
        "spark.databricks.delta.snapshotPartitions": "2",
        "spark.sql.sources.parallelPartitionDiscovery.parallelism": "2",
    }
    sessao = criar_sessao(
        "testes", driver_memory="1g", nucleos="2", particoes_shuffle=2, extras=rapido
    )
    yield sessao
    sessao.stop()
