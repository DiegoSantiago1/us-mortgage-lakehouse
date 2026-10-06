"""Cria a SparkSession local com Delta Lake.

Na imagem Docker, os jars do Delta ficam em HMDA_JARS (baixados uma vez no build),
então rodar o pipeline não depende de internet. Fora da imagem, o delta-spark
resolve os jars pelo Maven na primeira execução.
"""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def criar_sessao(
    nome: str = "hmda",
    driver_memory: str = "6g",
    nucleos: str = "*",
    particoes_shuffle: int = 48,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(nome)
        .master(f"local[{nucleos}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Datas e horas sem depender do fuso de quem roda (lição do Projeto 3).
        .config("spark.sql.session.timeZone", "UTC")
        # O padrão (200) é pensado para cluster; numa máquina, gera arquivos minúsculos.
        .config("spark.sql.shuffle.partitions", str(particoes_shuffle))
        .config("spark.ui.showConsoleProgress", "false")
    )
    pasta_jars = os.environ.get("HMDA_JARS", "")
    if pasta_jars and Path(pasta_jars).is_dir():
        jars = sorted(str(j) for j in Path(pasta_jars).glob("*.jar"))
        builder = builder.config("spark.jars", ",".join(jars))
    else:
        builder = configure_spark_with_delta_pip(builder)
    sessao = builder.getOrCreate()
    sessao.sparkContext.setLogLevel("WARN")
    return sessao
