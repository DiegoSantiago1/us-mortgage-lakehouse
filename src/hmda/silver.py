"""Silver: a versão vigente de cada ano, tipada e validada (D26, D28, D29).

    python -m hmda.silver --escopo     # aplica as versões na ordem em que o FFIEC publicou

- **Só a vigente:** cada ano tem uma versão na tabela (a mais madura carregada).
  Uma versão nova do mesmo ano substitui a anterior com `replaceWhere ano = X`,
  num commit próprio. As versões antigas continuam acessíveis por time travel,
  e `controle/silver_versoes` diz qual commit corresponde a qual versão do governo.
- **Tipos e códigos:** "NA", "Exempt" e vazio viram nulo (nunca zero). Código
  fora do domínio oficial, número ilegível ou linha malformada = **rejeição**
  (vai para `silver/rejeitados` com o motivo). Incoerência de negócio
  (ex.: motivo de negativa em empréstimo aprovado) = **alerta**: a linha fica,
  porque o número oficial a conta.
- **CHECK constraints do Delta** garantem os domínios na própria tabela, mesmo
  que alguém escreva nela sem passar por este código (defesa em camadas).
"""

import argparse
import sys
import time
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hmda import config, tabelas
from hmda.esquema import COLUNAS_LAR
from hmda.fontes import VERSOES, Fonte, congelado_em, escopo, ordem_de_publicacao

ESPECIAIS = ("", "NA", "Exempt")

# Domínios oficiais (ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields).
DOMINIOS: dict[str, tuple[int, ...]] = {
    "action_taken": (1, 2, 3, 4, 5, 6, 7, 8),
    "loan_purpose": (1, 2, 31, 32, 4, 5),
    "loan_type": (1, 2, 3, 4),
    "lien_status": (1, 2),
    "occupancy_type": (1, 2, 3),
    "construction_method": (1, 2),
    "preapproval": (1, 2),
    "purchaser_type": (0, 1, 2, 3, 4, 5, 6, 71, 72, 8, 9),
    "hoepa_status": (1, 2, 3),
    "applicant_sex": (1, 2, 3, 4, 6),
    "co_applicant_sex": (1, 2, 3, 4, 5, 6),
    "applicant_credit_score_type": (1, 2, 3, 4, 5, 6, 7, 8, 9),  # 1111 = isento: vira nulo
    "denial_reason_1": (1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    "denial_reason_2": (1, 2, 3, 4, 5, 6, 7, 8, 9),
    "denial_reason_3": (1, 2, 3, 4, 5, 6, 7, 8, 9),
    "denial_reason_4": (1, 2, 3, 4, 5, 6, 7, 8, 9),
}
# Campos "1 = sim, 2 = não, 1111 = isento".
SIM_NAO = (
    "reverse_mortgage",
    "open_end_line_of_credit",
    "business_or_commercial_purpose",
    "negative_amortization",
    "interest_only_payment",
    "balloon_payment",
    "other_nonamortizing_features",
)
FAIXAS_DTI = ("<20%", "20%-<30%", "30%-<36%", "50%-60%", ">60%")
FAIXAS_IDADE = ("<25", "25-34", "35-44", "45-54", "55-64", "65-74", ">74")
UNIDADES = ("1", "2", "3", "4", "5-24", "25-49", "50-99", "100-149", ">149")
LIMITE_CONFORME = ("C", "NC", "U")


def _grupo_dti(bruto: Column) -> Column:
    """Agrupa o DTI em faixas ordenáveis (o público traz faixas e, de 36 a 49, o valor exato)."""
    n = bruto.try_cast("int")
    return (
        F.when(bruto == "<20%", "<20%")
        .when(bruto == "20%-<30%", "20-30%")
        .when(bruto == "30%-<36%", "30-36%")
        .when(n.between(36, 39), "36-40%")
        .when(n.between(40, 44), "40-45%")
        .when(n.between(45, 49), "45-50%")
        .when(bruto == "50%-60%", "50-60%")
        .when(bruto == ">60%", ">60%")
    )


# Campos codificados em que 1111 = "isento" (Exempt). Nos campos numéricos, a
# isenção vem como o texto "Exempt" e 1111 pode ser um valor legítimo
# (ex.: renda de US$ 1,111 milhão), então lá ele NÃO é tratado como isenção.
CODIGOS_COM_ISENCAO = (
    *SIM_NAO,
    "applicant_credit_score_type",
    *(f"denial_reason_{i}" for i in range(1, 5)),  # medido: 302.921 em 2018
)


def _limpar(bronze: DataFrame, com_bruto: bool) -> DataFrame:
    """Estágio 1: "", NA e Exempt viram nulo, uma vez por coluna (D33)."""
    colunas = []
    for c in COLUNAS_LAR:
        especiais = (*ESPECIAIS, "1111") if c in CODIGOS_COM_ISENCAO else ESPECIAIS
        colunas.append(F.when(F.col(c).isin(*especiais), F.lit(None)).otherwise(F.col(c)).alias(c))
    # Cadeia de OR (compilável). `exists(array, lambda)` seria mais curto, mas funções
    # com lambda rodam sempre interpretadas no Spark: era o gargalo da silver (D36).
    isencao = reduce(
        lambda a, b: a | b,
        [F.col(c).eqNullSafe("Exempt") for c in COLUNAS_LAR]
        + [F.col(c).eqNullSafe("1111") for c in CODIGOS_COM_ISENCAO],
    )
    extras = [
        F.struct(*[c for c in bronze.columns if c not in ("_ano", "_versao")]).alias("_bruto")
    ]
    return bronze.select(
        *colunas,
        F.coalesce(isencao, F.lit(False)).alias("tem_isencao"),
        "_registro_corrompido",
        F.col("_ano").alias("ano"),
        F.col("_versao").alias("versao"),
        *(extras if com_bruto else []),
    )


# (coluna do FFIEC, coluna da silver, tipo): texto | int | decimal(...) | codigo | sim_nao
CAMPOS: tuple[tuple[str, str, str], ...] = (
    ("lei", "lei", "texto"),
    ("state_code", "estado", "texto"),
    ("county_code", "condado", "texto"),
    ("derived_msa_md", "msa", "texto"),
    ("census_tract", "setor_censitario", "texto"),
    ("action_taken", "resultado", "codigo"),
    ("loan_purpose", "finalidade", "codigo"),
    ("loan_type", "tipo_emprestimo", "codigo"),
    ("lien_status", "prioridade_garantia", "codigo"),
    ("occupancy_type", "ocupacao", "codigo"),
    ("construction_method", "metodo_construcao", "codigo"),
    ("preapproval", "pre_aprovacao", "codigo"),
    ("purchaser_type", "comprador", "codigo"),
    ("hoepa_status", "hoepa", "codigo"),
    ("total_units", "unidades", "texto"),
    ("conforming_loan_limit", "limite_conforme", "texto"),
    ("derived_loan_product_type", "produto", "texto"),
    ("derived_dwelling_category", "categoria_moradia", "texto"),
    ("reverse_mortgage", "hipoteca_reversa", "sim_nao"),
    ("open_end_line_of_credit", "linha_de_credito", "sim_nao"),
    ("business_or_commercial_purpose", "fim_comercial", "sim_nao"),
    ("negative_amortization", "amortizacao_negativa", "sim_nao"),
    ("interest_only_payment", "so_juros", "sim_nao"),
    ("balloon_payment", "parcela_balao", "sim_nao"),
    ("other_nonamortizing_features", "outras_nao_amortizantes", "sim_nao"),
    ("loan_amount", "valor_emprestimo", "decimal(18,2)"),
    ("property_value", "valor_imovel", "decimal(18,2)"),
    ("income", "renda_milhares", "int"),
    ("combined_loan_to_value_ratio", "ltv", "decimal(18,3)"),
    ("interest_rate", "taxa_juros", "decimal(18,3)"),
    ("rate_spread", "spread_taxa", "decimal(18,3)"),
    ("total_loan_costs", "custos_totais", "decimal(18,2)"),
    ("total_points_and_fees", "pontos_e_taxas", "decimal(18,2)"),
    ("origination_charges", "encargos_originacao", "decimal(18,2)"),
    ("discount_points", "pontos_desconto", "decimal(18,2)"),
    ("lender_credits", "creditos_credor", "decimal(18,2)"),
    ("loan_term", "prazo_meses", "int"),
    ("prepayment_penalty_term", "prazo_multa_meses", "int"),
    ("intro_rate_period", "periodo_taxa_inicial_meses", "int"),
    ("multifamily_affordable_units", "unidades_acessiveis_pct", "int"),
    ("debt_to_income_ratio", "dti_faixa", "texto"),
    ("applicant_age", "idade_faixa", "texto"),
    ("derived_race", "raca", "texto"),
    ("derived_ethnicity", "etnia", "texto"),
    ("derived_sex", "sexo", "texto"),
    ("applicant_sex", "sexo_solicitante", "codigo"),
    ("co_applicant_sex", "sexo_cosolicitante", "codigo"),
    ("applicant_credit_score_type", "tipo_score", "codigo"),
    ("denial_reason_1", "motivo_negativa_1", "codigo"),
    ("denial_reason_2", "motivo_negativa_2", "codigo"),
    ("denial_reason_3", "motivo_negativa_3", "codigo"),
    ("denial_reason_4", "motivo_negativa_4", "codigo"),
    ("tract_population", "populacao_setor", "int"),
    ("tract_minority_population_percent", "pct_minoria_setor", "decimal(10,2)"),
    ("ffiec_msa_md_median_family_income", "renda_mediana_msa", "int"),
    ("tract_to_msa_income_percentage", "pct_renda_setor_msa", "decimal(12,2)"),
)
TIPO_DE = {destino: tipo for _, destino, tipo in CAMPOS}
TEXTO_COM_DOMINIO = {
    "total_units": UNIDADES,
    "conforming_loan_limit": LIMITE_CONFORME,
    # 9999 é o código "sem cosolicitante" da idade do cosolicitante; aparece na do
    # solicitante em 80 linhas de 2018 (fora da documentação): fica, como nulo + alerta.
    "applicant_age": (*FAIXAS_IDADE, "8888", "9999"),
}


SEP = "|"


def _lista(itens: list[Column]) -> Column:
    """Lista dos itens não nulos, sem função de ordem superior (array_compact roda interpretado)."""
    texto = F.concat_ws(SEP, *itens)
    return F.when(texto == "", F.array().cast("array<string>")).otherwise(F.split(texto, r"\|"))


def _tipar(df: DataFrame) -> DataFrame:
    """Estágio 2: cada coluna limpa vira o seu tipo, uma vez (try_cast: ilegível = nulo)."""
    tipadas = []
    for origem, destino, tipo in CAMPOS:
        c = F.col(origem)
        if tipo == "texto":
            v = c
        elif tipo == "codigo":
            v = c.try_cast("int")
        elif tipo == "sim_nao":
            v = F.when(c == "1", F.lit(True)).when(c == "2", F.lit(False))
        else:
            v = c.try_cast(tipo)
        tipadas.append(v.alias(destino))
    # Colunas com o mesmo nome na origem e no destino (ex.: lei) ficam com a tipada.
    origem_mantida = [c for c in df.columns if c not in TIPO_DE]
    return df.select(
        *origem_mantida, *tipadas, _grupo_dti(F.col("debt_to_income_ratio")).alias("dti_grupo")
    )


def _validar(df: DataFrame, com_bruto: bool) -> DataFrame:
    """Estágio 3: motivos de rejeição e alertas, comparando colunas já prontas."""
    motivos = [
        F.when(F.col("_registro_corrompido").isNotNull(), F.lit("linha malformada")),
        F.when(
            F.col("activity_year").isNull()
            | (F.col("activity_year") != F.col("ano").cast("string")),
            F.lit("activity_year diferente do arquivo"),
        ),
    ]
    for origem, destino, tipo in CAMPOS:
        if origem == destino:
            continue  # "lei": texto livre, sem domínio
        presente = F.col(origem).isNotNull()
        if tipo == "codigo":
            ruim = presente & (F.col(destino).isNull() | ~F.col(destino).isin(*DOMINIOS[origem]))
            motivos.append(F.when(ruim, F.lit(f"{origem} fora do domínio")))
        elif tipo == "sim_nao":
            motivos.append(
                F.when(presente & F.col(destino).isNull(), F.lit(f"{origem} fora do domínio"))
            )
        elif origem in TEXTO_COM_DOMINIO:
            ruim = presente & ~F.col(origem).isin(*TEXTO_COM_DOMINIO[origem])
            motivos.append(F.when(ruim, F.lit(f"{origem} fora do domínio")))
        elif tipo != "texto":
            motivos.append(
                F.when(presente & F.col(destino).isNull(), F.lit(f"{origem} não numérico"))
            )
    # Todo DTI válido cai num grupo; valor presente sem grupo = fora do domínio.
    motivos.append(
        F.when(
            F.col("debt_to_income_ratio").isNotNull() & F.col("dti_grupo").isNull(),
            F.lit("debt_to_income_ratio fora do domínio"),
        )
    )

    alertas = _lista(
        [
            F.when(
                F.col("motivo_negativa_1").isNotNull()
                & (F.col("motivo_negativa_1") != 10)
                & ~F.col("resultado").isin(3, 7),
                F.lit("motivo de negativa sem negativa"),
            ),
            # Juros só são informados para originado (1), aprovado (2) e comprado (6).
            F.when(
                F.col("taxa_juros").isNotNull() & ~F.col("resultado").isin(1, 2, 6),
                F.lit("juros em pedido sem aprovação"),
            ),
            F.when(F.col("estado").isNull(), F.lit("sem estado")),
            F.when(F.col("idade_faixa") == "9999", F.lit("idade 9999 fora da documentação")),
            # Número legível mas implausível (medido: juros de 260000%) não é rejeitado:
            # o FFIEC publica e conta a linha. Fica marcado para as análises filtrarem.
            F.when(
                (F.col("taxa_juros") < 0) | (F.col("taxa_juros") > 30),
                F.lit("juros implausível"),
            ),
            F.when(F.abs(F.col("spread_taxa")) > 50, F.lit("spread implausível")),
        ]
    )
    finais = []
    for _, destino, tipo in CAMPOS:
        coluna = F.col(destino)
        if destino == "motivo_negativa_1":  # 10 = "não se aplica"
            coluna = F.when(coluna == 10, F.lit(None)).otherwise(coluna)
        elif destino == "idade_faixa":  # 8888 = "não se aplica"; 9999 = ver alerta
            coluna = F.when(coluna.isin("8888", "9999"), F.lit(None)).otherwise(coluna)
        if tipo == "codigo":
            coluna = coluna.cast("smallint")
        finais.append(coluna.alias(destino))
    return df.select(
        "ano",
        "versao",
        *finais,
        "dti_grupo",
        "tem_isencao",
        (F.col("sexo_cosolicitante") != 5).alias("tem_cosolicitante"),
        alertas.alias("alertas"),
        # Texto (vazio = linha válida): concat_ws pula os nulos e é compilável (D36).
        F.concat_ws(SEP, *motivos).alias("_rejeicoes"),
        *(["_bruto"] if com_bruto else []),
    )


def transformar(bronze: DataFrame, com_bruto: bool = True) -> DataFrame:
    """Bronze (texto) -> silver tipada, em três estágios (limpar, tipar, validar).

    Cada valor é calculado uma vez e os estágios seguintes só referenciam
    colunas prontas. A primeira versão repetia a limpeza dentro de cada regra:
    o código gerado ficou grande demais, o Spark caiu para o modo interpretado
    e processava ~1.900 linhas/s por núcleo (medido, D33).
    """
    return _validar(_tipar(_limpar(bronze, com_bruto)), com_bruto)


# CHECK constraints: espelham as regras de rejeição (D29).
RESTRICOES = {
    "ano_valido": "ano >= 2018",
    "versao_valida": "versao IN ('snapshot', 'one_year', 'three_year')",
    "resultado_valido": "resultado BETWEEN 1 AND 8",
    "finalidade_valida": "finalidade IS NULL OR finalidade IN (1, 2, 31, 32, 4, 5)",
    "tipo_valido": "tipo_emprestimo IS NULL OR tipo_emprestimo BETWEEN 1 AND 4",
    "garantia_valida": "prioridade_garantia IS NULL OR prioridade_garantia IN (1, 2)",
    "ocupacao_valida": "ocupacao IS NULL OR ocupacao IN (1, 2, 3)",
    "negativa_valida": "motivo_negativa_1 IS NULL OR motivo_negativa_1 BETWEEN 1 AND 9",
    "dti_valido": (
        "dti_faixa IS NULL OR dti_faixa IN ('<20%', '20%-<30%', '30%-<36%', '50%-60%', '>60%') "
        "OR (try_cast(dti_faixa AS INT) BETWEEN 36 AND 49)"
    ),
}

ESQUEMA_VERSOES = StructType(
    [
        StructField("ano", IntegerType()),
        StructField("versao", StringType()),
        StructField("congelado_em", DateType()),
        StructField("versao_delta", LongType()),
        StructField("linhas", LongType()),
        StructField("rejeitadas", LongType()),
        StructField("com_alerta", LongType()),
        StructField("aplicado_em", TimestampType()),
        StructField("segundos", IntegerType()),
    ]
)


def aplicar_restricoes(spark: SparkSession, caminho: str) -> None:
    existentes = {
        k.removeprefix("delta.constraints.")
        for k in DeltaTable.forPath(spark, caminho).detail().collect()[0]["properties"]
        if k.startswith("delta.constraints.")
    }
    for nome, regra in RESTRICOES.items():
        if nome.lower() not in existentes:
            spark.sql(f"ALTER TABLE delta.`{caminho}` ADD CONSTRAINT {nome} CHECK ({regra})")


def versao_vigente(spark: SparkSession, raiz: Path, ano: int) -> dict[str, Any] | None:
    caminho = tabelas.caminho(raiz, tabelas.SILVER_VERSOES)
    if not DeltaTable.isDeltaTable(spark, caminho):
        return None
    linhas = (
        spark.read.format("delta")
        .load(caminho)
        .where(F.col("ano") == ano)
        .orderBy(F.col("aplicado_em").desc())
        .limit(1)
        .collect()
    )
    return linhas[0].asDict() if linhas else None


def aplicar(spark: SparkSession, fonte: Fonte, raiz: Path, forcar: bool = False) -> dict[str, Any]:
    """Torna `fonte` a versão vigente do seu ano na silver."""
    atual = versao_vigente(spark, raiz, fonte.ano)
    if atual and not forcar:
        if atual["versao"] == fonte.versao:
            print(f"{fonte.chave}: já é a vigente, nada a fazer")
            return atual
        if VERSOES.index(atual["versao"]) > fonte.ordem:
            print(f"{fonte.chave}: a vigente ({atual['versao']}) é mais madura, nada a fazer")
            return atual

    t0 = time.monotonic()
    bronze = (
        spark.read.format("delta")
        .load(tabelas.caminho(raiz, tabelas.BRONZE))
        .where((F.col("_ano") == fonte.ano) & (F.col("_versao") == fonte.versao))
    )
    total = bronze.count()  # o Delta responde pelas estatísticas, sem ler os dados
    if total == 0:
        raise ValueError(f"{fonte.chave}: não está na bronze (rode hmda.bronze antes)")
    # Uma passada para as válidas, sem o texto original (D33). Os rejeitados só
    # são gravados (2ª passada, com o texto original) se existirem.
    df = transformar(bronze, com_bruto=False).withColumn("congelado_em", F.lit(congelado_em(fonte)))
    validas = df.where(F.col("_rejeicoes") == "").drop("_rejeicoes")

    caminho = tabelas.caminho(raiz, tabelas.SILVER)
    filtro = f"ano = {fonte.ano}"  # Fonte já validou o ano (int)
    escrita = validas.write.format("delta").partitionBy("ano")
    nova = not DeltaTable.isDeltaTable(spark, caminho)
    if nova:
        escrita.mode("errorifexists").save(caminho)
    else:
        escrita.mode("overwrite").option("replaceWhere", filtro).save(caminho)
    # Lido logo depois de gravar: é o commit que "é" esta versão do governo.
    versao_delta = DeltaTable.forPath(spark, caminho).history(1).collect()[0]["version"]
    if nova:
        aplicar_restricoes(spark, caminho)

    gravada = spark.read.format("delta").option("versionAsOf", versao_delta).load(caminho)
    gravada = gravada.where(F.col("ano") == fonte.ano)
    n_validas = gravada.count()
    n_alerta = gravada.where(F.size("alertas") > 0).count()

    caminho_rej = tabelas.caminho(raiz, tabelas.REJEITADOS)
    filtro_rej = f"ano = {fonte.ano} AND versao = '{fonte.versao}'"
    if total - n_validas > 0 or DeltaTable.isDeltaTable(spark, caminho_rej):
        rejeitadas = (
            transformar(bronze, com_bruto=True)
            .where(F.col("_rejeicoes") != "")
            .select("ano", "versao", F.split("_rejeicoes", r"\|").alias("_rejeicoes"), "_bruto")
        )
        if total - n_validas == 0:
            rejeitadas = rejeitadas.limit(0)  # só limpa a partição de uma carga anterior
        escrita_rej = rejeitadas.write.format("delta").partitionBy("ano", "versao")
        if DeltaTable.isDeltaTable(spark, caminho_rej):
            escrita_rej.mode("overwrite").option("replaceWhere", filtro_rej).save(caminho_rej)
        else:
            escrita_rej.mode("errorifexists").save(caminho_rej)
    n_rejeitadas = (
        spark.read.format("delta").load(caminho_rej).where(filtro_rej).count()
        if DeltaTable.isDeltaTable(spark, caminho_rej)
        else 0
    )
    if n_validas + n_rejeitadas != total:
        raise RuntimeError(
            f"{fonte.chave}: bronze {total:,} != silver {n_validas:,} + rejeitadas {n_rejeitadas:,}"
        )

    registro = {
        "ano": fonte.ano,
        "versao": fonte.versao,
        "congelado_em": congelado_em(fonte),
        "versao_delta": versao_delta,
        "linhas": n_validas,
        "rejeitadas": n_rejeitadas,
        "com_alerta": n_alerta,
        "aplicado_em": datetime.now(UTC).replace(tzinfo=None),
        "segundos": round(time.monotonic() - t0),
    }
    spark.createDataFrame([registro], ESQUEMA_VERSOES).write.format("delta").mode("append").save(
        tabelas.caminho(raiz, tabelas.SILVER_VERSOES)
    )
    print(
        f"{fonte.chave}: vigente na versão Delta {versao_delta}; "
        f"{registro['linhas']:,} linhas, {n_rejeitadas:,} rejeitadas, {n_alerta:,} com alerta "
        f"({registro['segundos']} s)"
    )
    return registro


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--ano", type=int)
    p.add_argument("--versao", choices=VERSOES)
    p.add_argument("--escopo", action="store_true")
    p.add_argument("--forcar", action="store_true")
    args = p.parse_args(argv)
    if args.escopo == (args.ano is not None or args.versao is not None):
        p.error("use --escopo OU --ano e --versao")

    cfg = config.carregar()
    from hmda.spark import criar_sessao

    spark = criar_sessao("silver", driver_memory=cfg.driver_memory)
    try:
        fontes = ordem_de_publicacao(escopo()) if args.escopo else [Fonte(args.ano, args.versao)]
        for fonte in fontes:
            aplicar(spark, fonte, cfg.tabelas, args.forcar)
    except ValueError as erro:
        print(f"erro: {erro}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
