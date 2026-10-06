"""Bronze: o CSV do FFIEC em Delta, tudo como texto, mais metadados da carga.

    python -m hmda.bronze --ano 2021 --versao three_year
    python -m hmda.bronze --escopo           # tudo o que foi baixado, em ordem

Garantias:
- **Idempotente:** se o mesmo arquivo (mesmo SHA-256) já foi carregado nessa
  partição, não faz nada. Arquivo novo para o mesmo ano/versão substitui só a
  partição (`replaceWhere`); as outras não são tocadas.
- **Nada se perde em silêncio:** as quebras de linha são contadas durante a
  extração do zip (conta independente do Spark). Se o que foi gravado não bate,
  a tabela volta à versão anterior (`RESTORE`) e a carga falha.
- **Linha malformada não some:** fica na bronze com o texto original em
  `_registro_corrompido`, para a silver rejeitar e a qualidade contar.
"""

import argparse
import shutil
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hmda import config, manifesto, tabelas
from hmda.esquema import COLUNAS_LAR
from hmda.fontes import VERSOES, Fonte, escopo

BLOCO = 8 * 1024 * 1024
COLUNA_CORROMPIDA = "_registro_corrompido"


class ErroCarga(RuntimeError):
    pass


@dataclass(frozen=True)
class Extraido:
    csv: Path
    cabecalho: list[str]
    linhas_de_dados: int


def extrair(zip_path: Path, destino: Path) -> Extraido:
    """Extrai o único CSV do zip contando as linhas no caminho.

    O `zipfile` confere o CRC-32 de cada membro ao terminar de ler: um zip
    corrompido (ou uma retomada de download errada) falha aqui.
    """
    try:
        with zipfile.ZipFile(zip_path) as z:
            csvs = [
                i
                for i in z.infolist()
                if i.filename.lower().endswith(".csv") and not i.filename.startswith("__MACOSX/")
            ]
            if len(csvs) != 1:
                nomes = [i.filename for i in z.infolist()]
                raise ErroCarga(f"esperava 1 CSV em {zip_path.name}, achei {nomes}")
            destino.mkdir(parents=True, exist_ok=True)
            csv_path = destino / Path(csvs[0].filename).name
            quebras = 0
            ultimo = b"\n"
            with z.open(csvs[0]) as origem, csv_path.open("wb") as saida:
                cabecalho = origem.readline()
                saida.write(cabecalho)
                while bloco := origem.read(BLOCO):
                    saida.write(bloco)
                    quebras += bloco.count(b"\n")
                    ultimo = bloco[-1:]
    except zipfile.BadZipFile as erro:
        raise ErroCarga(f"zip inválido ou corrompido ({zip_path.name}): {erro}") from erro

    # Última linha sem "\n" no fim também é uma linha.
    linhas = quebras + (0 if ultimo == b"\n" else 1)
    colunas = cabecalho.decode("utf-8").rstrip("\r\n").split(",")
    return Extraido(csv=csv_path, cabecalho=colunas, linhas_de_dados=linhas)


def validar_cabecalho(colunas: list[str]) -> None:
    if tuple(colunas) == COLUNAS_LAR:
        return
    sobram = [c for c in colunas if c not in COLUNAS_LAR]
    faltam = [c for c in COLUNAS_LAR if c not in colunas]
    detalhe = (
        f"sobram {sobram}, faltam {faltam}" if sobram or faltam else "mesma lista, ordem diferente"
    )
    raise ErroCarga(f"cabeçalho diferente do esperado (D27): {detalhe}")


ESQUEMA_CSV = StructType(
    [StructField(c, StringType()) for c in COLUNAS_LAR]
    + [StructField(COLUNA_CORROMPIDA, StringType())]
)

ESQUEMA_CARGAS = StructType(
    [
        StructField("ano", IntegerType()),
        StructField("versao", StringType()),
        StructField("arquivo", StringType()),
        StructField("sha256", StringType()),
        StructField("linhas_csv", LongType()),
        StructField("linhas_bronze", LongType()),
        StructField("corrompidas", LongType()),
        StructField("versao_delta", LongType()),
        StructField("inicio", TimestampType()),
        StructField("segundos", IntegerType()),
    ]
)


def ler_csv(spark: SparkSession, csv: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", COLUNA_CORROMPIDA)
        .schema(ESQUEMA_CSV)
        .csv(str(csv))
    )


def ultima_carga(spark: SparkSession, raiz: Path, fonte: Fonte) -> dict[str, Any] | None:
    caminho = tabelas.caminho(raiz, tabelas.CARGAS)
    if not DeltaTable.isDeltaTable(spark, caminho):
        return None
    linhas = (
        spark.read.format("delta")
        .load(caminho)
        .where((F.col("ano") == fonte.ano) & (F.col("versao") == fonte.versao))
        .orderBy(F.col("inicio").desc())
        .limit(1)
        .collect()
    )
    return linhas[0].asDict() if linhas else None


def _filtro(fonte: Fonte) -> str:
    # Fonte já validou ano (int) e versão (lista fechada): sem risco de injeção.
    return f"_ano = {fonte.ano} AND _versao = '{fonte.versao}'"


def carregar(
    spark: SparkSession,
    fonte: Fonte,
    zip_path: Path,
    sha256: str,
    raiz: Path,
    temporario: Path,
    forcar: bool = False,
) -> dict[str, Any]:
    anterior = ultima_carga(spark, raiz, fonte)
    caminho = tabelas.caminho(raiz, tabelas.BRONZE)
    if anterior and anterior["sha256"] == sha256 and not forcar:
        print(f"{fonte.chave}: já está na bronze ({sha256[:12]}), nada a fazer")
        return anterior

    inicio = datetime.now(UTC)
    t0 = time.monotonic()
    pasta_tmp = temporario / f"{fonte.ano}_{fonte.versao}"
    shutil.rmtree(pasta_tmp, ignore_errors=True)
    try:
        print(f"{fonte.chave}: extraindo {zip_path.name}")
        ext = extrair(zip_path, pasta_tmp)
        validar_cabecalho(ext.cabecalho)
        print(f"{fonte.chave}: {ext.linhas_de_dados:,} linhas no CSV; gravando a bronze")

        df = ler_csv(spark, ext.csv).select(
            *COLUNAS_LAR,
            COLUNA_CORROMPIDA,
            F.lit(fonte.ano).cast("int").alias("_ano"),
            F.lit(fonte.versao).alias("_versao"),
            F.lit(zip_path.name).alias("_arquivo"),
            F.lit(sha256).alias("_sha256_arquivo"),
            F.lit(inicio).cast("timestamp").alias("_carregado_em"),
        )

        existe = DeltaTable.isDeltaTable(spark, caminho)
        versao_antes = (
            DeltaTable.forPath(spark, caminho).history(1).collect()[0]["version"]
            if existe
            else None
        )
        escrita = df.write.format("delta").partitionBy("_ano", "_versao")
        if existe:
            escrita.mode("overwrite").option("replaceWhere", _filtro(fonte)).save(caminho)
        else:
            escrita.mode("errorifexists").save(caminho)

        tabela = DeltaTable.forPath(spark, caminho)
        versao_delta = tabela.history(1).collect()[0]["version"]
        contagem = (
            tabela.toDF()
            .where(_filtro(fonte))
            .agg(
                F.count("*").alias("linhas"),
                F.count(COLUNA_CORROMPIDA).alias("corrompidas"),
            )
            .collect()[0]
        )
        if contagem["linhas"] != ext.linhas_de_dados:
            if versao_antes is not None:
                tabela.restoreToVersion(versao_antes)
            raise ErroCarga(
                f"{fonte.chave}: o CSV tem {ext.linhas_de_dados:,} linhas e a bronze gravou "
                f"{contagem['linhas']:,}; tabela restaurada para a versão {versao_antes}"
            )
    finally:
        shutil.rmtree(pasta_tmp, ignore_errors=True)

    registro = {
        "ano": fonte.ano,
        "versao": fonte.versao,
        "arquivo": zip_path.name,
        "sha256": sha256,
        "linhas_csv": ext.linhas_de_dados,
        "linhas_bronze": contagem["linhas"],
        "corrompidas": contagem["corrompidas"],
        "versao_delta": versao_delta,
        "inicio": inicio.replace(tzinfo=None),
        "segundos": round(time.monotonic() - t0),
    }
    spark.createDataFrame([registro], ESQUEMA_CARGAS).write.format("delta").mode("append").save(
        tabelas.caminho(raiz, tabelas.CARGAS)
    )
    print(
        f"{fonte.chave}: ok, {registro['linhas_bronze']:,} linhas "
        f"({registro['corrompidas']} malformadas), versão Delta {versao_delta}, "
        f"{registro['segundos']} s"
    )
    return registro


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--ano", type=int)
    p.add_argument("--versao", choices=VERSOES)
    p.add_argument("--escopo", action="store_true", help="carrega tudo o que foi baixado")
    p.add_argument("--forcar", action="store_true", help="recarrega mesmo com o mesmo hash")
    args = p.parse_args(argv)
    if args.escopo == (args.ano is not None or args.versao is not None):
        p.error("use --escopo OU --ano e --versao")

    cfg = config.carregar()
    dados = manifesto.carregar(cfg.manifesto)
    from hmda.spark import criar_sessao

    spark = criar_sessao("bronze", driver_memory=cfg.driver_memory)
    try:
        fontes = escopo() if args.escopo else [Fonte(args.ano, args.versao)]
        for fonte in fontes:
            atual = manifesto.arquivo_atual(dados, fonte.chave)
            if atual is None:
                raise ErroCarga(f"{fonte.chave}: não baixado (rode hmda.baixar antes)")
            zip_path = cfg.bruto / str(fonte.ano) / fonte.versao / atual["arquivo"]
            carregar(
                spark, fonte, zip_path, atual["sha256"], cfg.tabelas, cfg.temporario, args.forcar
            )
    except (ValueError, ErroCarga) as erro:
        print(f"erro: {erro}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
