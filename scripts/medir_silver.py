"""Mede onde vai o tempo da silver (leitura, transformação, escrita) num ano real.

python scripts/medir_silver.py 2018 three_year
"""

import sys
import tempfile
import time

from pyspark.sql import functions as F

from hmda import config, silver, tabelas
from hmda.spark import criar_sessao


def cronometrar(rotulo, funcao):
    t = time.monotonic()
    resultado = funcao()
    print(f"{rotulo:<45} {time.monotonic() - t:7.1f} s  -> {resultado}")
    return resultado


def main(ano: int, versao: str) -> None:
    cfg = config.carregar()
    spark = criar_sessao("medir_silver", driver_memory=cfg.driver_memory)
    bronze = (
        spark.read.format("delta")
        .load(tabelas.caminho(cfg.tabelas, tabelas.BRONZE))
        .where((F.col("_ano") == ano) & (F.col("_versao") == versao))
    )
    cronometrar("count da bronze (estatísticas)", bronze.count)
    cronometrar(
        "ler todas as colunas (soma de tamanhos)",
        lambda: bronze.select(
            F.sum(F.length(F.concat_ws("|", *[c for c in bronze.columns if not c.startswith("_")])))
        ).collect()[0][0],
    )
    df = silver.transformar(bronze, com_bruto=False)
    cronometrar(
        "transformar + filtrar válidas (count)", lambda: df.where(F.col("_rejeicoes") == "").count()
    )
    with tempfile.TemporaryDirectory(dir="/lake") as pasta:
        cronometrar(
            "transformar + gravar Delta",
            lambda: (
                df.where(F.col("_rejeicoes") == "")
                .drop("_rejeicoes")
                .write.format("delta")
                .save(pasta + "/t")
            ),
        )
    spark.stop()


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2])
