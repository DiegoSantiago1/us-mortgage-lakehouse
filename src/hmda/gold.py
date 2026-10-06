"""Gold: as tabelas que respondem às perguntas do projeto (fase 4).

    python -m hmda.gold        # só roda se a qualidade da silver estiver aprovada

Todas as análises usam **recortes explícitos**, para comparar o comparável:
- recorte_moradia: 1ª hipoteca, crédito fechado (sem linha de crédito), sem hipoteca
  reversa, sem fim comercial, moradia própria, casa de 1 a 4 unidades;
- recorte_compra: o anterior + finalidade compra + construção no local (sem casa móvel).

Tabelas:
- preco_renda   mediana de valor do imóvel / renda (compras originadas) por ano e lugar
- mercado       originações, volume e juros medianos por finalidade, ano e lugar
- negativas     pedidos decididos (1, 2, 3), negados (3) e taxa, por finalidade, ano e lugar
- motivos       motivos citados nas negativas (um pedido pode citar até 4)
- equidade      negativas observadas x esperadas para quem tem perfil parecido (D32)
- impacto       as mesmas métricas no Snapshot e na versão final (time travel, D28)
"""

import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from hmda import config, dq, tabelas

FINALIDADES = {
    1: "compra",
    2: "reforma",
    31: "refinanciamento",
    32: "refinanciamento com saque",
    4: "outra",
}
MOTIVOS = {
    1: "dívida/renda (DTI)",
    2: "histórico de emprego",
    3: "histórico de crédito",
    4: "garantia (imóvel)",
    5: "falta de dinheiro",
    6: "informação não verificável",
    7: "pedido incompleto",
    8: "seguro hipotecário negado",
    9: "outro",
}
CELULA_MINIMA = 30  # pedidos numa célula de perfil para ela entrar na comparação
GRUPO_MINIMO = 1000  # pedidos de um grupo para ele ser publicado
MSA_MINIMA = 100  # compras numa região metropolitana para ela entrar no preço/renda


def _rotulo(mapa: dict[int, str], coluna: str) -> Column:
    expr = F.lit(None).cast("string")
    for k, v in mapa.items():
        expr = F.when(F.col(coluna) == k, F.lit(v)).otherwise(expr)
    return expr


def recorte_moradia(df: DataFrame) -> DataFrame:
    return df.where(
        (F.col("prioridade_garantia") == 1)
        & (F.col("linha_de_credito") == False)  # noqa: E712 (nulo = isento: fica fora)
        & (F.col("hipoteca_reversa") == False)  # noqa: E712
        & (F.col("fim_comercial") == False)  # noqa: E712
        & (F.col("ocupacao") == 1)
        & F.col("unidades").isin("1", "2", "3", "4")
    )


def recorte_compra(df: DataFrame) -> DataFrame:
    return recorte_moradia(df).where((F.col("finalidade") == 1) & (F.col("metodo_construcao") == 1))


def _por_lugar(df: DataFrame, aggs: list[Column], extras: tuple[str, ...] = ()) -> DataFrame:
    """Mesmas métricas em três níveis: nacional, estado e região metropolitana."""
    nac = (
        df.groupBy("ano", *extras)
        .agg(*aggs)
        .withColumns({"nivel": F.lit("nacional"), "lugar": F.lit("US")})
    )
    est = (
        df.where(F.col("estado").isNotNull())
        .groupBy("ano", "estado", *extras)
        .agg(*aggs)
        .withColumnsRenamed({"estado": "lugar"})
        .withColumn("nivel", F.lit("estado"))
    )
    msa = (
        df.where(F.col("msa").isNotNull() & (F.col("msa") != "99999"))
        .groupBy("ano", "msa", *extras)
        .agg(*aggs)
        .withColumnsRenamed({"msa": "lugar"})
        .withColumn("nivel", F.lit("msa"))
    )
    return nac.unionByName(est).unionByName(msa)


def preco_renda(silver: DataFrame) -> DataFrame:
    base = (
        recorte_compra(silver)
        .where(
            (F.col("resultado") == 1) & (F.col("renda_milhares") > 0) & (F.col("valor_imovel") > 0)
        )
        .withColumn("razao", F.col("valor_imovel") / (F.col("renda_milhares") * 1000))
    )
    aggs = [
        F.count("*").alias("compras"),
        F.median("razao").alias("preco_renda"),
        F.median("valor_imovel").alias("valor_imovel_mediano"),
        (F.median("renda_milhares") * 1000).alias("renda_mediana"),
    ]
    r = _por_lugar(base, aggs)
    return r.where((F.col("nivel") != "msa") | (F.col("compras") >= MSA_MINIMA))


def mercado(silver: DataFrame) -> DataFrame:
    base = recorte_moradia(silver).where(F.col("resultado") == 1)
    base = base.withColumn("finalidade_nome", _rotulo(FINALIDADES, "finalidade"))
    aggs = [
        F.count("*").alias("originacoes"),
        F.sum("valor_emprestimo").alias("volume"),
        F.median("taxa_juros").alias("juros_mediano"),
    ]
    return _por_lugar(base, aggs, ("finalidade_nome",)).where(F.col("finalidade_nome").isNotNull())


def negativas(silver: DataFrame) -> DataFrame:
    base = recorte_moradia(silver).where(F.col("resultado").isin(1, 2, 3))
    base = base.withColumn("finalidade_nome", _rotulo(FINALIDADES, "finalidade"))
    aggs = [
        F.count("*").alias("pedidos"),
        F.sum((F.col("resultado") == 3).cast("int")).alias("negados"),
    ]
    r = _por_lugar(base, aggs, ("finalidade_nome",)).where(F.col("finalidade_nome").isNotNull())
    return r.withColumn("taxa_negativa", F.col("negados") / F.col("pedidos"))


def motivos(silver: DataFrame) -> DataFrame:
    negados = (
        recorte_moradia(silver)
        .where(F.col("resultado") == 3)
        .withColumn("finalidade_nome", _rotulo(FINALIDADES, "finalidade"))
    )
    totais = negados.groupBy("ano", "finalidade_nome").agg(F.count("*").alias("negados"))
    citados = (
        negados.select(
            "ano",
            "finalidade_nome",
            F.explode(
                F.array_distinct(
                    F.array_compact(F.array(*[F.col(f"motivo_negativa_{i}") for i in range(1, 5)]))
                )
            ).alias("motivo"),
        )
        .groupBy("ano", "finalidade_nome", "motivo")
        .agg(F.count("*").alias("citacoes"))
    )
    return (
        citados.join(totais, ["ano", "finalidade_nome"])
        .withColumn("pct_dos_negados", F.col("citacoes") / F.col("negados"))
        .withColumn("motivo_nome", _rotulo(MOTIVOS, "motivo"))
        .where(F.col("finalidade_nome").isNotNull())
    )


def _faixa(coluna: str, limites: list[float], rotulos: list[str]) -> Column:
    expr = F.lit(rotulos[-1])
    for limite, rotulo in reversed(list(zip(limites, rotulos, strict=False))):
        expr = F.when(F.col(coluna) < limite, F.lit(rotulo)).otherwise(expr)
    return F.when(F.col(coluna).isNull(), F.lit(None)).otherwise(expr)


def grupo_racial(raca: Column, etnia: Column) -> Column:
    """Como os relatórios do CFPB: hispânico de qualquer raça primeiro; depois a raça."""
    return (
        F.when(etnia == "Hispanic or Latino", "Hispânico")
        .when(raca == "Black or African American", "Negro")
        .when(raca == "Asian", "Asiático")
        .when((raca == "White") & (etnia == "Not Hispanic or Latino"), "Branco não hispânico")
        .when(raca == "American Indian or Alaska Native", "Indígena")
        .when(raca == "Native Hawaiian or Other Pacific Islander", "Havaiano ou Ilhas do Pacífico")
        .when(raca == "Joint", "Conjunto (raças diferentes)")
        .when(raca == "2 or more minority races", "Duas ou mais minorias")
        .otherwise("Não informado")
    )


CELULA = ("estado", "tipo_emprestimo", "faixa_renda", "dti_grupo", "faixa_ltv", "faixa_valor")


def equidade(silver: DataFrame) -> DataFrame:
    """Negativas observadas ÷ esperadas, com o perfil controlado (D32).

    Esperado de um grupo = soma, em cada célula de perfil parecido, de
    (pedidos do grupo na célula × taxa de negativa de TODOS na célula).
    Razão 1,30 = o grupo teve 30% mais negativas do que o perfil dele explicaria.
    Não há score de crédito no dado público: diferença não prova discriminação.
    """
    base = (
        recorte_compra(silver)
        .where(F.col("resultado").isin(1, 2, 3))
        .select(
            "ano",
            "estado",
            "tipo_emprestimo",
            "dti_grupo",
            (F.col("resultado") == 3).cast("int").alias("negado"),
            _faixa(
                "renda_milhares",
                [50, 75, 100, 150, 250],
                ["<50 mil", "50-75 mil", "75-100 mil", "100-150 mil", "150-250 mil", "250 mil+"],
            ).alias("faixa_renda"),
            _faixa(
                "ltv",
                [60, 80, 90, 95, 100.001],
                ["<60", "60-80", "80-90", "90-95", "95-100", ">100"],
            ).alias("faixa_ltv"),
            _faixa(
                "valor_emprestimo",
                [150_000, 250_000, 400_000, 650_000],
                ["<150 mil", "150-250 mil", "250-400 mil", "400-650 mil", "650 mil+"],
            ).alias("faixa_valor"),
            grupo_racial(F.col("raca"), F.col("etnia")).alias("grupo_racial"),
            F.when(F.col("sexo") == "Male", "Homem")
            .when(F.col("sexo") == "Female", "Mulher")
            .when(F.col("sexo") == "Joint", "Conjunto")
            .otherwise("Não informado")
            .alias("sexo_grupo"),
        )
    )
    completos = base.dropna(subset=list(CELULA))
    celulas = (
        completos.groupBy("ano", *CELULA)
        .agg(F.count("*").alias("n_celula"), F.sum("negado").alias("neg_celula"))
        .where(F.col("n_celula") >= CELULA_MINIMA)
        .withColumn("taxa_celula", F.col("neg_celula") / F.col("n_celula"))
    )
    comparaveis = completos.join(celulas, ["ano", *CELULA])
    geral = comparaveis.groupBy("ano").agg(
        (F.sum("negado") / F.count("*")).alias("taxa_geral"),
        F.count("*").alias("pedidos_comparaveis"),
    )
    cobertura = base.groupBy("ano").agg(F.count("*").alias("pedidos_no_recorte"))

    resultado = None
    for dimensao, coluna in (("raça/etnia", "grupo_racial"), ("sexo", "sexo_grupo")):
        bruto = base.groupBy("ano", F.col(coluna).alias("grupo")).agg(
            (F.sum("negado") / F.count("*")).alias("taxa_bruta")
        )
        controlado = comparaveis.groupBy("ano", F.col(coluna).alias("grupo")).agg(
            F.count("*").alias("pedidos"),
            F.sum("negado").alias("negados"),
            F.sum("taxa_celula").alias("esperados"),
        )
        parte = (
            controlado.join(bruto, ["ano", "grupo"])
            .join(geral, "ano")
            .join(cobertura, "ano")
            .withColumn("dimensao", F.lit(dimensao))
        )
        resultado = parte if resultado is None else resultado.unionByName(parte)

    assert resultado is not None
    return (
        resultado.withColumn("razao_obs_esp", F.col("negados") / F.col("esperados"))
        .withColumn("taxa_ajustada", F.col("razao_obs_esp") * F.col("taxa_geral"))
        .withColumn("taxa_observada", F.col("negados") / F.col("pedidos"))
        .withColumn(
            "publicavel",
            (F.col("pedidos") >= GRUPO_MINIMO) & (F.col("grupo") != "Não informado"),
        )
    )


def metricas_nacionais(silver: DataFrame) -> DataFrame:
    """Um punhado de números nacionais por ano, para medir o efeito das revisões."""
    pr = (
        preco_renda(silver)
        .where(F.col("nivel") == "nacional")
        .select("ano", "compras", "preco_renda")
    )
    ng = (
        negativas(silver)
        .where((F.col("nivel") == "nacional") & (F.col("finalidade_nome") == "compra"))
        .select("ano", F.col("taxa_negativa").alias("taxa_negativa_compra"))
    )
    tot = silver.groupBy("ano").agg(
        F.count("*").alias("registros"),
        F.sum((F.col("resultado") == 1).cast("int")).alias("originados"),
        F.sum(F.when(F.col("resultado") == 1, F.col("valor_emprestimo"))).alias("volume_originado"),
    )
    return tot.join(pr, "ano", "left").join(ng, "ano", "left")


def impacto(spark: SparkSession, raiz: Path) -> DataFrame:
    """Métricas de 2021 e 2022 em cada versão do governo, via time travel da silver."""
    versoes = (
        spark.read.format("delta")
        .load(tabelas.caminho(raiz, tabelas.SILVER_VERSOES))
        .where(F.col("ano").isin(2021, 2022))
        .collect()
    )
    partes = []
    for v in versoes:
        antiga = (
            spark.read.format("delta")
            .option("versionAsOf", v["versao_delta"])
            .load(tabelas.caminho(raiz, tabelas.SILVER))
            .where(F.col("ano") == v["ano"])
        )
        partes.append(
            metricas_nacionais(antiga).withColumns(
                {
                    "versao": F.lit(v["versao"]),
                    "congelado_em": F.lit(v["congelado_em"]),
                    "versao_delta": F.lit(v["versao_delta"]),
                }
            )
        )
    resultado = partes[0]
    for p in partes[1:]:
        resultado = resultado.unionByName(p)
    return resultado


TABELAS_GOLD = {
    "preco_renda": preco_renda,
    "mercado": mercado,
    "negativas": negativas,
    "motivos": motivos,
    "equidade": equidade,
}


def construir(spark: SparkSession, raiz: Path) -> dict[str, int]:
    dq.exigir_aprovacao(spark, raiz)
    # Sem persist(): a silver tem ~136 mi de linhas; o Delta é colunar e cada
    # tabela lê só as colunas que usa.
    silver = spark.read.format("delta").load(tabelas.caminho(raiz, tabelas.SILVER))
    linhas = {}
    for nome, funcao in TABELAS_GOLD.items():
        df = funcao(silver)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            tabelas.caminho(raiz, f"gold/{nome}")
        )
        linhas[nome] = (
            spark.read.format("delta").load(tabelas.caminho(raiz, f"gold/{nome}")).count()
        )
        print(f"gold/{nome}: {linhas[nome]:,} linhas")
    impacto(spark, raiz).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(tabelas.caminho(raiz, "gold/impacto"))
    print("gold/impacto: ok")
    return linhas


def main() -> int:
    cfg = config.carregar()
    from hmda.spark import criar_sessao

    spark = criar_sessao("gold", driver_memory=cfg.driver_memory)
    try:
        construir(spark, cfg.tabelas)
    except dq.DadoReprovado as erro:
        print(f"erro: {erro}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
