"""Catálogo de tabelas: as tabelas Delta com nome, consultadas em SQL (fase 7, D41).

Até aqui cada tabela era um caminho (D16). Este módulo registra os mesmos caminhos num
catálogo, como tabelas externas: `silver.vigente` aponta para /lake/tabelas/silver/vigente.
Apagar a tabela do catálogo não apaga os dados; o catálogo guarda só nome, esquema e local.

Localmente o catálogo é um metastore Hive (Derby embutido, numa pasta do volume): as
consultas são HiveQL/Spark SQL e o registro sobrevive entre execuções. No Databricks, o
mesmo papel é do Unity Catalog (databricks/README.md).

Uso:
    python -m hmda.catalogo                  registra as tabelas e lista o catálogo
    python -m hmda.catalogo --relatorio docs/CATALOGO.md
        registra e grava a demonstração: EXPLAIN e arquivos lidos com e sem filtro
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from hmda import tabelas
from hmda.config import carregar
from hmda.spark import criar_sessao

# Nome lógico (tabelas.py) -> (esquema, tabela) no catálogo. "camada/nome" vira camada.nome.
_NOME_VALIDO = re.compile(r"[a-z_][a-z0-9_]*")


@dataclass(frozen=True)
class Leitura:
    """O que uma consulta lê de verdade: arquivos e pastas de partição."""

    arquivos: int
    pastas: tuple[str, ...]


def nome_no_catalogo(nome_logico: str) -> tuple[str, str]:
    """'silver/vigente' -> ('silver', 'vigente'). Recusa nome que exigiria aspas no SQL."""
    partes = nome_logico.split("/")
    if len(partes) != 2 or not all(_NOME_VALIDO.fullmatch(p) for p in partes):
        raise ValueError(f"nome lógico fora do padrão camada/tabela: {nome_logico!r}")
    return partes[0], partes[1]


def tabelas_existentes(raiz: Path) -> list[str]:
    """Nomes lógicos (camada/tabela) que têm uma tabela Delta gravada sob a raiz."""
    return sorted(
        str(log.parent.relative_to(raiz)).replace("\\", "/")
        for log in raiz.glob("*/*/_delta_log")
        if log.is_dir()
    )


def registrar(spark: SparkSession, raiz: Path) -> list[str]:
    """Registra cada tabela Delta da raiz como tabela externa. Idempotente.

    Se o local de uma tabela já registrada mudou (outra raiz), o registro é refeito: o
    catálogo nunca aponta para um lugar diferente do que está no disco.
    """
    registradas = []
    for logico in tabelas_existentes(raiz):
        esquema, tabela = nome_no_catalogo(logico)
        local = tabelas.caminho(raiz, logico)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema}")
        nome = f"{esquema}.{tabela}"
        if spark.catalog.tableExists(nome):
            atual = spark.sql(f"DESCRIBE DETAIL {nome}").first()
            if atual is not None and str(atual["location"]).rstrip("/").endswith(local.rstrip("/")):
                registradas.append(nome)
                continue
            spark.sql(f"DROP TABLE {nome}")  # externa: some só o registro, os dados ficam
        # Local entre aspas simples: vem do disco (glob), e o nome já foi validado acima.
        spark.sql(f"CREATE TABLE {nome} USING DELTA LOCATION '{local}'")
        registradas.append(nome)
    return registradas


def leitura(spark: SparkSession, sql: str) -> Leitura:
    """Arquivos que a consulta vai ler, depois da poda de partições e do data skipping."""
    arquivos = spark.sql(sql).inputFiles()
    pastas = sorted({m.group(0) for a in arquivos if (m := re.search(r"ano=\d{4}", a))})
    return Leitura(arquivos=len(arquivos), pastas=tuple(pastas))


def explicar(spark: SparkSession, sql: str) -> str:
    """Plano físico formatado (EXPLAIN FORMATTED), como o Spark vai executar a consulta."""
    linha = spark.sql(f"EXPLAIN FORMATTED {sql}").first()
    return "" if linha is None else str(linha[0])


def filtros_de_particao(plano: str) -> list[str]:
    """Linhas 'PartitionFilters: [...]' do plano: o que o Spark usa para pular pastas."""
    return [m.strip() for m in re.findall(r"PartitionFilters: \[[^\]]*\]", plano)]


# Consultas da demonstração: mesma pergunta, filtros diferentes.
CONSULTAS = {
    "filtro na partição (ano = 2022)": (
        "SELECT estado, COUNT(*) AS pedidos FROM silver.vigente WHERE ano = 2022 GROUP BY estado"
    ),
    "filtro em coluna comum (estado = 'TX')": (
        "SELECT ano, COUNT(*) AS pedidos FROM silver.vigente WHERE estado = 'TX' GROUP BY ano"
    ),
    "sem filtro": "SELECT ano, COUNT(*) AS pedidos FROM silver.vigente GROUP BY ano",
}


def relatorio(spark: SparkSession, destino: Path) -> str:
    """Grava a demonstração em Markdown, com os números medidos nesta execução."""
    detalhe = spark.sql("DESCRIBE DETAIL silver.vigente").first()
    assert detalhe is not None
    total = leitura(spark, CONSULTAS["sem filtro"])
    linhas = [
        "# Catálogo de tabelas e poda de partições (gerado por `python -m hmda.catalogo`)",
        "",
        "Tabelas registradas no metastore Hive local (externas: o catálogo guarda nome, "
        "esquema e local; os dados ficam no Delta):",
        "",
    ]
    for esquema in sorted(r[0] for r in spark.sql("SHOW SCHEMAS").collect()):
        if esquema == "default":
            continue
        for t in spark.sql(f"SHOW TABLES IN {esquema}").collect():
            linhas.append(f"- `{esquema}.{t['tableName']}`")
    linhas += [
        "",
        f"`silver.vigente`: {detalhe['numFiles']} arquivos Parquet, "
        f"{detalhe['sizeInBytes'] / 1e9:.1f} GB, particionada por "
        f"`{', '.join(detalhe['partitionColumns'])}`.",
        "",
        "| Consulta | Arquivos lidos | Pastas de partição lidas |",
        "|---|---|---|",
    ]
    planos = {}
    for rotulo, sql in CONSULTAS.items():
        lido = leitura(spark, sql)
        planos[rotulo] = explicar(spark, sql)
        pct = 100 * lido.arquivos / total.arquivos if total.arquivos else 0
        linhas.append(
            f"| {rotulo} | {lido.arquivos} de {total.arquivos} ({pct:.0f}%) | "
            f"{len(lido.pastas)}: {', '.join(lido.pastas)} |"
        )
    linhas += ["", "## O que o plano mostra", ""]
    for rotulo, plano in planos.items():
        filtros = filtros_de_particao(plano) or ["(nenhum filtro de partição: lê todas as pastas)"]
        colunas = re.findall(r"ReadSchema: .*", plano)[:1]
        linhas += [f"**{rotulo}**", "", "```", *filtros, *colunas, "```", ""]
    rotulo = "filtro na partição (ano = 2022)"
    linhas += [
        "## Como ler",
        "",
        "- **Poda de partições:** `ano` é a coluna de partição, então cada ano é uma pasta "
        "(`ano=2022/`). Com `WHERE ano = 2022`, o Spark escolhe as pastas antes de abrir "
        "qualquer arquivo (`PartitionFilters` no plano).",
        "- **Poda de colunas:** `ReadSchema` lista só as colunas que a consulta usa. O Parquet "
        "guarda cada coluna separada, então as outras nem saem do disco.",
        "- **Filtro em coluna comum:** `estado` não é partição e todo arquivo tem pedidos de "
        "todos os estados, então o mínimo e o máximo guardados pelo Delta não deixam pular "
        "nenhum arquivo. Ordenar os dados por estado dentro de cada ano (Z-ORDER ou liquid "
        "clustering) faria esse filtro pular arquivos também; não foi feito porque as "
        "análises daqui agregam o país inteiro.",
        "",
    ]
    linhas += [
        f"## Plano completo: {rotulo}",
        "",
        "```sql",
        CONSULTAS[rotulo],
        "```",
        "",
        "```",
        planos[rotulo].strip(),
        "```",
        "",
    ]
    texto = "\n".join(linhas)
    destino.write_text(texto, encoding="utf-8", newline="\n")
    return texto


def sessao_com_catalogo(pasta_metastore: Path, driver_memory: str = "6g") -> SparkSession:
    """SparkSession com metastore Hive persistente (Derby embutido) na pasta indicada."""
    pasta_metastore.mkdir(parents=True, exist_ok=True)
    return criar_sessao(
        "hmda-catalogo",
        driver_memory=driver_memory,
        extras={
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": str(pasta_metastore / "warehouse"),
            "spark.hadoop.javax.jdo.option.ConnectionURL": (
                f"jdbc:derby:;databaseName={pasta_metastore / 'metastore_db'};create=true"
            ),
            # O Derby grava o log dele na pasta atual se não disser onde.
            "spark.driver.extraJavaOptions": f"-Dderby.system.home={pasta_metastore}",
        },
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--relatorio", type=Path, help="grava a demonstração neste arquivo")
    args = parser.parse_args(argv)
    config = carregar()
    raiz = config.tabelas
    spark = sessao_com_catalogo(config.lake / "metastore", config.driver_memory)
    try:
        for nome in registrar(spark, raiz):
            print(f"registrada: {nome}")
        if args.relatorio:
            relatorio(spark, args.relatorio)
            print(f"relatório: {args.relatorio}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
