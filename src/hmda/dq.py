"""Qualidade de dados da silver vigente: checagens que BLOQUEIAM (D12).

    python -m hmda.dq        # sai com código 1 se alguma checagem falhar

Cada execução grava o resultado em `dq/checagens`. A gold e o exportador do
site chamam `exigir_aprovacao()` e se recusam a rodar sobre dado reprovado
(mesmo princípio do Projeto 3).

Checagens, por ano:
- versao_unica         uma versão do governo por ano, a registrada em controle/silver_versoes
- conservacao          bronze da versão = silver + rejeitados (nenhuma linha some)
- sem_rejeicao         zero linhas rejeitadas (o FFIEC publica dado validado)
- oficial              silver por estado x resultado = API do FFIEC, célula por célula (D24)
- essenciais           lei, resultado, finalidade e tipo de empréstimo sem nulo
E uma para a tabela: restricoes (as CHECK constraints estão lá).
"""

import sys
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hmda import config, oficial, tabelas
from hmda.silver import RESTRICOES

ESQUEMA = StructType(
    [
        StructField("executado_em", TimestampType()),
        StructField("ano", IntegerType()),
        StructField("checagem", StringType()),
        StructField("passou", BooleanType()),
        StructField("detalhe", StringType()),
    ]
)


class DadoReprovado(RuntimeError):
    pass


def _ler(spark: SparkSession, raiz: Path, nome: str):
    return spark.read.format("delta").load(tabelas.caminho(raiz, nome))


def verificar(spark: SparkSession, raiz: Path, pasta_oficial: Path = oficial.PASTA) -> list[dict]:
    agora = datetime.now(UTC).replace(tzinfo=None)
    resultados: list[dict] = []

    def registrar(ano: int | None, nome: str, passou: bool, detalhe: str) -> None:
        resultados.append(
            {
                "executado_em": agora,
                "ano": ano,
                "checagem": nome,
                "passou": passou,
                "detalhe": detalhe,
            }
        )

    caminho = tabelas.caminho(raiz, tabelas.SILVER)
    props = DeltaTable.forPath(spark, caminho).detail().collect()[0]["properties"]
    faltam = [n for n in RESTRICOES if f"delta.constraints.{n.lower()}" not in props]
    registrar(None, "restricoes", not faltam, f"faltam: {faltam}" if faltam else "9 de 9")

    silver = _ler(spark, raiz, tabelas.SILVER)
    versoes_tabela = {
        (r["ano"], r["versao"]): r["count"]
        for r in silver.groupBy("ano", "versao").count().collect()
    }
    registro = {}
    for r in _ler(spark, raiz, tabelas.SILVER_VERSOES).orderBy("aplicado_em").collect():
        registro[r["ano"]] = r["versao"]  # o último aplicado de cada ano
    bronze = {
        (r["_ano"], r["_versao"]): r["count"]
        for r in _ler(spark, raiz, tabelas.BRONZE).groupBy("_ano", "_versao").count().collect()
    }
    rej_path = tabelas.caminho(raiz, tabelas.REJEITADOS)
    rejeitados = (
        {
            (r["ano"], r["versao"]): r["count"]
            for r in _ler(spark, raiz, tabelas.REJEITADOS)
            .groupBy("ano", "versao")
            .count()
            .collect()
        }
        if DeltaTable.isDeltaTable(spark, rej_path)
        else {}
    )
    por_celula: dict[int, dict[tuple[str, str], int]] = {}
    for r in (
        silver.where(F.col("estado").isNotNull())
        .groupBy("ano", "estado", "resultado")
        .count()
        .collect()
    ):
        por_celula.setdefault(r["ano"], {})[(r["estado"], str(r["resultado"]))] = r["count"]
    nulos = {
        r["ano"]: r.asDict()
        for r in silver.groupBy("ano")
        .agg(
            *[
                F.sum(F.col(c).isNull().cast("int")).alias(c)
                for c in ("lei", "resultado", "finalidade", "tipo_emprestimo")
            ]
        )
        .collect()
    }

    for ano in sorted({a for a, _ in versoes_tabela}):
        versoes = [v for (a, v) in versoes_tabela if a == ano]
        versao = registro.get(ano)
        registrar(
            ano,
            "versao_unica",
            versoes == [versao],
            f"na tabela: {versoes}; registrada: {versao}",
        )
        n_silver = versoes_tabela[(ano, versoes[0])]
        n_bronze = bronze.get((ano, versoes[0]), 0)
        n_rej = rejeitados.get((ano, versoes[0]), 0)
        registrar(
            ano,
            "conservacao",
            n_bronze == n_silver + n_rej,
            f"bronze {n_bronze:,} = silver {n_silver:,} + rejeitados {n_rej:,}",
        )
        registrar(ano, "sem_rejeicao", n_rej == 0, f"{n_rej:,} rejeitadas")

        arquivo = pasta_oficial / f"contagens_{ano}.json"
        if not arquivo.exists():
            registrar(ano, "oficial", False, f"sem número oficial em {arquivo.name}")
        else:
            ofc = oficial.carregar(ano, pasta_oficial)["por_estado"]
            esperado = {
                (e, a): int(v["contagem"]) for e, acoes in ofc.items() for a, v in acoes.items()
            }
            obtido = por_celula.get(ano, {})
            celulas = set(esperado) | set(obtido)
            dif = sorted(
                (c, obtido.get(c, 0) - esperado.get(c, 0))
                for c in celulas
                if obtido.get(c, 0) != esperado.get(c, 0)
            )
            total = sum(esperado.values())
            registrar(
                ano,
                "oficial",
                not dif,
                f"{len(celulas)} células, total oficial {total:,}"
                + (f"; diferenças (estado, resultado, silver-oficial): {dif[:10]}" if dif else ""),
            )

        zerados = {c: v for c, v in nulos.get(ano, {}).items() if c != "ano" and v}
        registrar(ano, "essenciais", not zerados, f"nulos: {zerados}" if zerados else "sem nulos")

    spark.createDataFrame(resultados, ESQUEMA).write.format("delta").mode("append").save(
        tabelas.caminho(raiz, tabelas.DQ)
    )
    return resultados


def exigir_aprovacao(spark: SparkSession, raiz: Path) -> None:
    """Para a gold e o exportador: a última rodada de checagens precisa estar 100% verde."""
    caminho = tabelas.caminho(raiz, tabelas.DQ)
    if not DeltaTable.isDeltaTable(spark, caminho):
        raise DadoReprovado("nenhuma checagem de qualidade rodou (python -m hmda.dq)")
    dq = spark.read.format("delta").load(caminho)
    ultima = dq.agg(F.max("executado_em")).collect()[0][0]
    falhas = dq.where((F.col("executado_em") == ultima) & ~F.col("passou")).collect()
    if falhas:
        lista = [f"{f['ano']} {f['checagem']}: {f['detalhe']}" for f in falhas]
        raise DadoReprovado("qualidade reprovada:\n  " + "\n  ".join(lista))
    ultima_silver = (
        DeltaTable.forPath(spark, tabelas.caminho(raiz, tabelas.SILVER)).history(1).collect()[0]
    )
    if ultima_silver["timestamp"] > ultima:
        raise DadoReprovado("a silver mudou depois da última checagem: rode python -m hmda.dq")


def main() -> int:
    cfg = config.carregar()
    from hmda.spark import criar_sessao

    spark = criar_sessao("dq", driver_memory=cfg.driver_memory)
    try:
        resultados = verificar(spark, cfg.tabelas)
    finally:
        spark.stop()
    for r in resultados:
        marca = "ok   " if r["passou"] else "FALHA"
        print(f"{marca} {r['ano'] or '-'} {r['checagem']:<13} {r['detalhe']}")
    return 0 if all(r["passou"] for r in resultados) else 1


if __name__ == "__main__":
    sys.exit(main())
