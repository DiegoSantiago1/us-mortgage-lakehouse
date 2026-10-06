"""Exporta a gold para os JSON da página de resultados (site/dados/).

    python -m hmda.exportar_site

Recusa-se a exportar se a qualidade da silver não estiver aprovada (D12).
A página nunca recebe dado linha a linha: só agregados, e os grupos da
análise de equidade só aparecem com n mínimo (D4).
"""

import csv
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from hmda import config, dq, tabelas

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
SAIDA = RAIZ_PROJETO / "site" / "dados"
REFERENCIA = RAIZ_PROJETO / "docs" / "referencia"


def _limpo(valor: Any) -> Any:
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, float):
        return round(valor, 6)
    if hasattr(valor, "isoformat"):
        return valor.isoformat()
    return valor


def linhas(df: DataFrame) -> list[dict[str, Any]]:
    return [{k: _limpo(v) for k, v in r.asDict().items()} for r in df.collect()]


def _ler(spark: SparkSession, raiz: Path, nome: str) -> DataFrame:
    return spark.read.format("delta").load(tabelas.caminho(raiz, nome))


def _nomes_msa() -> dict[str, dict[str, str]]:
    nomes: dict[str, dict[str, str]] = {}
    for ano in (2018, 2024):
        with (REFERENCIA / f"msa_{ano}.csv").open(encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                nomes.setdefault(r["msa_md"], {"nome": r["msa_md_name"].title(), "uf": r["state"]})
    return nomes


def _nomes_instituicoes() -> dict[str, str]:
    nomes: dict[str, str] = {}
    for arq in sorted(REFERENCIA.glob("instituicoes_*.csv")):
        with arq.open(encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                nomes[r["lei"]] = r["nome"]
    return nomes


def montar(spark: SparkSession, raiz: Path) -> dict[str, Any]:
    pr = _ler(spark, raiz, "gold/preco_renda")
    msa = _nomes_msa()
    preco_msa = []
    for r in linhas(pr.where(F.col("nivel") == "msa")):
        info = msa.get(r["lugar"])
        if info:
            preco_msa.append(r | info)

    mercado = _ler(spark, raiz, "gold/mercado").where(F.col("nivel") != "msa")
    negativas = _ler(spark, raiz, "gold/negativas").where(F.col("nivel") != "msa")
    equidade = _ler(spark, raiz, "gold/equidade").where(F.col("publicavel"))

    instituicoes = _nomes_instituicoes()
    rev_inst = (
        _ler(spark, raiz, tabelas.REV_INSTITUICOES)
        .where((F.col("de_versao") == "snapshot") & (F.col("para_versao") == "three_year"))
        .withColumn("mexidas", F.col("entraram") + F.col("sairam"))
    )
    top_inst = []
    for ano in (2021, 2022):
        for r in linhas(rev_inst.where(F.col("ano") == ano).orderBy(F.desc("mexidas")).limit(12)):
            r["nome"] = instituicoes.get(r["lei"], r["lei"])
            top_inst.append(r)
    resumo_rev = linhas(
        _ler(spark, raiz, tabelas.REV_DIFERENCAS)
        .groupBy("ano", "de_versao", "para_versao")
        .agg(F.sum("entrou").alias("entraram"), F.sum("saiu").alias("sairam"))
    )
    inst_por_par = linhas(
        rev_inst.groupBy("ano").agg(
            F.count("*").alias("instituicoes"),
            F.sum((F.col("mexidas") > 0).cast("int")).alias("instituicoes_que_mudaram"),
            F.sum("antes").alias("antes"),
            F.sum("depois").alias("depois"),
        )
    )

    cargas = linhas(_ler(spark, raiz, tabelas.CARGAS).orderBy("ano", "versao"))
    ultima_dq = _ler(spark, raiz, tabelas.DQ)
    quando = ultima_dq.agg(F.max("executado_em")).collect()[0][0]

    return {
        "gerado_em": datetime.now(UTC).isoformat(timespec="seconds"),
        "fonte": "FFIEC / CFPB, HMDA Modified LAR (ffiec.cfpb.gov)",
        "cargas": cargas,
        "versoes_silver": linhas(_ler(spark, raiz, tabelas.SILVER_VERSOES).orderBy("aplicado_em")),
        "qualidade": linhas(
            ultima_dq.where(F.col("executado_em") == quando).orderBy("ano", "checagem")
        ),
        "preco_renda": {
            "nacional": linhas(pr.where(F.col("nivel") == "nacional").orderBy("ano")),
            "estados": linhas(pr.where(F.col("nivel") == "estado").orderBy("ano", "lugar")),
            "msa": preco_msa,
        },
        "mercado": linhas(mercado.orderBy("ano", "nivel", "lugar", "finalidade_nome")),
        "negativas": linhas(negativas.orderBy("ano", "nivel", "lugar", "finalidade_nome")),
        "motivos": linhas(
            _ler(spark, raiz, "gold/motivos").orderBy("ano", "finalidade_nome", "motivo")
        ),
        "equidade": linhas(equidade.orderBy("ano", "dimensao", "grupo")),
        "revisoes": {
            "resumo": resumo_rev,
            "instituicoes": inst_por_par,
            "top_instituicoes": top_inst,
            "campos": linhas(
                _ler(spark, raiz, tabelas.REV_CAMPOS).orderBy("ano", "de_versao", F.desc("pares"))
            ),
            "impacto": linhas(_ler(spark, raiz, "gold/impacto").orderBy("ano", "congelado_em")),
        },
    }


def main() -> int:
    cfg = config.carregar()
    from hmda.spark import criar_sessao

    spark = criar_sessao("exportar", driver_memory=cfg.driver_memory)
    try:
        dq.exigir_aprovacao(spark, cfg.tabelas)
        dados = montar(spark, cfg.tabelas)
    except dq.DadoReprovado as erro:
        print(f"erro: {erro}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    SAIDA.mkdir(parents=True, exist_ok=True)
    destino = SAIDA / "resultados.json"
    destino.write_text(
        json.dumps(dados, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"{destino}: {destino.stat().st_size / 1024:,.0f} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
