"""Amostra da silver para o Databricks Free Edition (fase 7, databricks/README.md).

O Databricks Free tem cota diária e armazenamento não documentado, e a internet dele só
alcança domínios aprovados (não baixa do FFIEC). Por isso sobe uma amostra pequena, com a
mesma estrutura: 1% de cada ano, sempre os mesmos pedidos (semente fixa), num arquivo
Parquet só (a tela de upload do Databricks não guarda pastas; a tabela é particionada por
ano lá dentro, no CREATE TABLE).

Uso:
    python -m hmda.amostra                 grava /dados/databricks/amostra_silver.parquet
    python -m hmda.amostra --fracao 0.005
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from hmda import tabelas
from hmda.config import carregar
from hmda.spark import criar_sessao

SEMENTE = 20261014


def amostrar(silver: DataFrame, fracao: float, semente: int = SEMENTE) -> DataFrame:
    """A mesma fração em todos os anos, sem as colunas internas (_bruto, _rejeicoes)."""
    if not 0 < fracao <= 1:
        raise ValueError(f"fração fora de (0, 1]: {fracao}")
    anos = [r["ano"] for r in silver.select("ano").distinct().collect()]
    colunas = [c for c in silver.columns if not c.startswith("_")]
    return silver.select(*colunas).sampleBy("ano", {a: fracao for a in anos}, seed=semente)


def gravar(spark: SparkSession, raiz: Path, destino: Path, fracao: float) -> int:
    """Grava a amostra num arquivo Parquet só, em `destino`."""
    silver = spark.read.format("delta").load(tabelas.caminho(raiz, tabelas.SILVER))
    pasta = destino.with_suffix(".tmp")
    amostrar(silver, fracao).coalesce(1).write.mode("overwrite").parquet(str(pasta))
    [parte] = pasta.glob("part-*.parquet")
    parte.replace(destino)
    shutil.rmtree(pasta)
    return spark.read.parquet(str(destino)).count()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Amostra da silver para o Databricks.")
    parser.add_argument("--fracao", type=float, default=0.01)
    args = parser.parse_args(argv)
    config = carregar()
    destino = config.dados / "databricks" / "amostra_silver.parquet"
    destino.parent.mkdir(parents=True, exist_ok=True)
    spark = criar_sessao("hmda-amostra", driver_memory=config.driver_memory)
    try:
        linhas = gravar(spark, config.tabelas, destino, args.fracao)
    finally:
        spark.stop()
    tamanho = destino.stat().st_size
    print(f"amostra: {linhas:,} linhas, {tamanho / 1e6:.1f} MB em {destino}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
