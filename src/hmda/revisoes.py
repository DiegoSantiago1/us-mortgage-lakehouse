"""Revisões: quanto o dado do governo muda entre versões do mesmo ano (D9).

    python -m hmda.revisoes            # 2021 e 2022: Snapshot -> One Year -> Three Year

O dado público não tem identificador do empréstimo. Então a comparação é entre
**multiconjuntos de linhas**: cada linha vira uma chave (hash do texto original
das 99 colunas) e conta-se quantas vezes cada chave aparece em cada versão.
    entrou = max(0, n_depois - n_antes)      saiu = max(0, n_antes - n_depois)
Linhas idênticas repetidas (comuns: sem ULI, dois pedidos iguais na mesma área
são indistinguíveis) são respeitadas pela contagem.

Uma correção aparece como "saiu uma, entrou outra". Para estimar **qual campo**
foi corrigido, compara-se cada linha que saiu com as que entraram ignorando uma
coluna por vez: se ficam iguais sem a coluna X, a diferença era só em X.

A exatidão do hash é conferida contra o `exceptAll` do Spark (diferença exata de
multiconjuntos, linha inteira, sem hash) num estado inteiro (`conferir_com_except_all`).
"""

import argparse
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from hmda import config, tabelas
from hmda.esquema import COLUNAS_LAR
from hmda.fontes import VERSOES

PARES = (("snapshot", "one_year"), ("one_year", "three_year"), ("snapshot", "three_year"))
ANOS_COM_VERSOES = (2021, 2022)
NULO = "\u0000"  # marca de nulo: ("a", nulo) e (nulo, "a") não podem dar o mesmo texto
SEPARADOR = "\u001f"


def texto_da_linha(colunas: list[str]) -> Column:
    return F.concat_ws(SEPARADOR, *[F.coalesce(F.col(c), F.lit(NULO)) for c in colunas])


def chave(colunas: list[str]) -> list[Column]:
    """96 bits: xxhash64 + murmur3 (32) do texto da linha. Colisão desprezível em 26 mi."""
    t = texto_da_linha(colunas)
    return [F.xxhash64(t).alias("h1"), F.hash(t).alias("h2")]


def versao_bronze(spark: SparkSession, raiz: Path, ano: int, versao: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(tabelas.caminho(raiz, tabelas.BRONZE))
        .where((F.col("_ano") == ano) & (F.col("_versao") == versao))
    )


def contagens(df: DataFrame) -> DataFrame:
    return (
        df.select(*chave(list(COLUNAS_LAR)), "lei", "state_code", "action_taken", "loan_purpose")
        .groupBy("h1", "h2")
        .agg(
            F.count("*").alias("n"),
            # Mesma chave = mesmo texto = mesmos atributos: first() é exato aqui.
            F.first("lei").alias("lei"),
            F.first("state_code").alias("estado"),
            F.first("action_taken").alias("resultado"),
            F.first("loan_purpose").alias("finalidade"),
        )
    )


def diferencas(antes: DataFrame, depois: DataFrame) -> DataFrame:
    a, d = contagens(antes).alias("a"), contagens(depois).alias("d")
    j = a.join(d, ["h1", "h2"], "full_outer")
    n_a = F.coalesce(F.col("a.n"), F.lit(0))
    n_d = F.coalesce(F.col("d.n"), F.lit(0))
    return j.select(
        "h1",
        "h2",
        *[
            F.coalesce(F.col(f"a.{c}"), F.col(f"d.{c}")).alias(c)
            for c in ("lei", "estado", "resultado", "finalidade")
        ],
        n_a.alias("n_antes"),
        n_d.alias("n_depois"),
        F.greatest(n_d - n_a, F.lit(0)).alias("entrou"),
        F.greatest(n_a - n_d, F.lit(0)).alias("saiu"),
        F.least(n_a, n_d).alias("iguais"),
    )


def linhas_que_mudaram(df: DataFrame, dif: DataFrame, lado: str) -> DataFrame:
    """As linhas de `df` que saíram (lado='saiu') ou entraram ('entrou'), com multiplicidade."""
    alvo = dif.where(F.col(lado) > 0).select("h1", "h2", F.col(lado).alias("vezes"))
    com_chave = df.select(*COLUNAS_LAR, *chave(list(COLUNAS_LAR)))
    juntas = com_chave.join(alvo, ["h1", "h2"])
    janela = Window.partitionBy("h1", "h2").orderBy(F.lit(1))
    return (
        juntas.withColumn("_i", F.row_number().over(janela))
        .where(F.col("_i") <= F.col("vezes"))
        .drop("_i", "vezes")
    )


def campos_corrigidos(saiu: DataFrame, entrou: DataFrame) -> DataFrame:
    """Para cada coluna X: quantos pares (saiu, entrou) ficam idênticos ignorando só X.

    Uma passada: um hash por coluna (com a posição, para a ordem importar),
    combinados por XOR. "A linha sem a coluna X" = total XOR hash(X), porque XOR
    é a própria inversa. Assim são 99 expressões pequenas, e não 99 hashes de 98
    colunas cada: esse primeiro jeito estourava a memória do Java só para gerar
    o código, mesmo com 3 linhas (D30).
    Depois conta-se, por (coluna, hash), quantas linhas de cada lado têm aquele
    hash; o número de pares é o mínimo dos dois lados.
    """

    def explodir(df: DataFrame, lado: str) -> DataFrame:
        por_coluna = F.array(
            *[
                F.struct(
                    F.lit(c).alias("campo"),
                    F.xxhash64(F.lit(i), F.coalesce(F.col(c), F.lit(NULO))).alias("hc"),
                )
                for i, c in enumerate(COLUNAS_LAR)
            ]
        )
        return (
            df.select(por_coluna.alias("hcs"))
            .select(
                "hcs",
                F.aggregate(
                    "hcs", F.lit(0).cast("long"), lambda acc, x: acc.bitwiseXOR(x["hc"])
                ).alias("total"),
            )
            .select(F.explode("hcs").alias("x"), "total")
            .select(
                F.col("x.campo").alias("campo"),
                F.col("total").bitwiseXOR(F.col("x.hc")).alias("h"),
                F.lit(lado).alias("lado"),
            )
        )

    todos = explodir(saiu, "saiu").unionByName(explodir(entrou, "entrou"))
    por_hash = todos.groupBy("campo", "h").agg(
        F.sum(F.when(F.col("lado") == "saiu", 1).otherwise(0)).alias("s"),
        F.sum(F.when(F.col("lado") == "entrou", 1).otherwise(0)).alias("e"),
    )
    return (
        por_hash.groupBy("campo")
        .agg(F.sum(F.least("s", "e")).alias("pares"))
        .where(F.col("pares") > 0)
    )


def conferir_com_except_all(antes: DataFrame, depois: DataFrame, estado: str) -> dict[str, int]:
    """Diferença exata (sem hash) num estado, para provar que o hash não erra."""
    a = antes.where(F.col("state_code") == estado).select(*COLUNAS_LAR)
    d = depois.where(F.col("state_code") == estado).select(*COLUNAS_LAR)
    exato_saiu = a.exceptAll(d).count()
    exato_entrou = d.exceptAll(a).count()
    pelo_hash = (
        diferencas(a, d)
        .agg(F.sum("saiu").alias("saiu"), F.sum("entrou").alias("entrou"))
        .collect()[0]
    )
    return {
        "exato_saiu": exato_saiu,
        "exato_entrou": exato_entrou,
        "hash_saiu": int(pelo_hash["saiu"] or 0),
        "hash_entrou": int(pelo_hash["entrou"] or 0),
    }


def _gravar(df: DataFrame, caminho: str, filtro: str, particoes: list[str]) -> None:
    escrita = df.write.format("delta").partitionBy(*particoes)
    if DeltaTable.isDeltaTable(df.sparkSession, caminho):
        escrita.mode("overwrite").option("replaceWhere", filtro).save(caminho)
    else:
        escrita.mode("errorifexists").save(caminho)


def comparar(
    spark: SparkSession, raiz: Path, ano: int, de: str, para: str, estado_conferencia: str = "DE"
) -> dict:
    if de not in VERSOES or para not in VERSOES:  # entra no filtro do replaceWhere
        raise ValueError(f"versão desconhecida: {de!r} ou {para!r}")
    antes = versao_bronze(spark, raiz, ano, de)
    depois = versao_bronze(spark, raiz, ano, para)
    dif = diferencas(antes, depois).persist()
    try:
        marca = [
            F.lit(ano).alias("ano"),
            F.lit(de).alias("de_versao"),
            F.lit(para).alias("para_versao"),
        ]
        filtro = f"ano = {int(ano)} AND de_versao = '{de}' AND para_versao = '{para}'"

        mudou = dif.where((F.col("entrou") > 0) | (F.col("saiu") > 0))
        _gravar(
            mudou.select(*marca, "*"),
            tabelas.caminho(raiz, tabelas.REV_DIFERENCAS),
            filtro,
            ["ano", "de_versao", "para_versao"],
        )
        por_inst = dif.groupBy("lei").agg(
            F.sum("n_antes").alias("antes"),
            F.sum("n_depois").alias("depois"),
            F.sum("entrou").alias("entraram"),
            F.sum("saiu").alias("sairam"),
        )
        _gravar(
            por_inst.select(*marca, "*"),
            tabelas.caminho(raiz, tabelas.REV_INSTITUICOES),
            filtro,
            ["ano"],
        )
        resumo = (
            dif.agg(
                F.sum("n_antes").alias("antes"),
                F.sum("n_depois").alias("depois"),
                F.sum("iguais").alias("iguais"),
                F.sum("entrou").alias("entraram"),
                F.sum("saiu").alias("sairam"),
            )
            .collect()[0]
            .asDict()
        )

        saiu = linhas_que_mudaram(antes, dif, "saiu")
        entrou = linhas_que_mudaram(depois, dif, "entrou")
        campos = campos_corrigidos(saiu, entrou)
        _gravar(
            campos.select(*marca, "*"),
            tabelas.caminho(raiz, tabelas.REV_CAMPOS),
            filtro,
            ["ano"],
        )
    finally:
        dif.unpersist()

    resumo = {k: int(v or 0) for k, v in resumo.items()}
    resumo["conferencia"] = conferir_com_except_all(antes, depois, estado_conferencia)
    resumo.update(ano=ano, de_versao=de, para_versao=para)
    print(
        f"{ano} {de} -> {para}: antes {resumo['antes']:,}, depois {resumo['depois']:,}, "
        f"iguais {resumo['iguais']:,}, entraram {resumo['entraram']:,}, saíram {resumo['sairam']:,}"
    )
    c = resumo["conferencia"]
    if (c["exato_saiu"], c["exato_entrou"]) != (c["hash_saiu"], c["hash_entrou"]):
        raise RuntimeError(f"hash diverge do exceptAll em {estado_conferencia}: {c}")
    return resumo


def main(argv: list[str] | None = None) -> int:
    import json

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--anos", type=int, nargs="+", default=list(ANOS_COM_VERSOES))
    args = p.parse_args(argv)
    cfg = config.carregar()
    from hmda.spark import criar_sessao

    spark = criar_sessao("revisoes", driver_memory=cfg.driver_memory)
    resumos = []
    try:
        for ano in args.anos:
            for de, para in PARES:
                resumos.append(comparar(spark, cfg.tabelas, ano, de, para))
    finally:
        spark.stop()
    saida = cfg.dados / "medicoes" / "revisoes_resumo.json"
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(json.dumps(resumos, indent=2) + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
