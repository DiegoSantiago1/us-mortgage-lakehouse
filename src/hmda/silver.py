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
    "applicant_credit_score_type": (1, 2, 3, 4, 5, 6, 7, 8, 9),
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


class _Construtor:
    """Monta as colunas tipadas e, ao mesmo tempo, a lista de motivos de rejeição."""

    def __init__(self) -> None:
        self.colunas: list[Column] = []
        self.rejeicoes: list[Column] = []
        self.isencoes: list[Column] = []

    @staticmethod
    def bruto(nome: str) -> Column:
        return F.trim(F.col(nome))

    def limpo(self, nome: str) -> Column:
        b = self.bruto(nome)
        if nome != "action_taken":
            self.isencoes.append(b.isin("Exempt", "1111"))
        return F.when(b.isNull() | b.isin(*ESPECIAIS), F.lit(None)).otherwise(b)

    def texto(self, nome: str, alias: str, permitidos: tuple[str, ...] | None = None) -> None:
        v = self.limpo(nome)
        if permitidos is not None:
            self.rejeicoes.append(
                F.when(v.isNotNull() & ~v.isin(*permitidos), F.lit(f"{nome} fora do domínio"))
            )
        self.colunas.append(v.alias(alias))

    def numero(self, nome: str, alias: str, tipo: str) -> None:
        v = self.limpo(nome)
        convertido = v.try_cast(tipo)
        self.rejeicoes.append(
            F.when(v.isNotNull() & convertido.isNull(), F.lit(f"{nome} não numérico"))
        )
        self.colunas.append(convertido.alias(alias))

    def codigo(self, nome: str, alias: str) -> None:
        v = self.limpo(nome).try_cast("int")
        self.rejeicoes.append(
            F.when(
                self.bruto(nome).isNotNull()
                & ~self.bruto(nome).isin(*ESPECIAIS)
                & (v.isNull() | ~v.isin(*DOMINIOS[nome])),
                F.lit(f"{nome} fora do domínio"),
            )
        )
        if nome == "denial_reason_1":
            v = F.when(v == 10, F.lit(None)).otherwise(v)  # 10 = "não se aplica"
        self.colunas.append(v.cast("smallint").alias(alias))

    def sim_nao(self, nome: str, alias: str) -> None:
        b = self.bruto(nome)
        self.isencoes.append(b.isin("Exempt", "1111"))
        self.rejeicoes.append(
            F.when(
                b.isNotNull() & ~b.isin("1", "2", "1111", *ESPECIAIS),
                F.lit(f"{nome} fora do domínio"),
            )
        )
        self.colunas.append(F.when(b == "1", F.lit(True)).when(b == "2", F.lit(False)).alias(alias))


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


def transformar(bronze: DataFrame) -> DataFrame:
    """Bronze (texto) -> colunas tipadas + `_rejeicoes` e `_alertas` (listas de motivos)."""
    c = _Construtor()
    c.colunas.append(F.col("_ano").alias("ano"))
    c.colunas.append(F.col("_versao").alias("versao"))
    c.rejeicoes.append(F.when(F.col("_registro_corrompido").isNotNull(), F.lit("linha malformada")))
    c.rejeicoes.append(
        F.when(
            F.col("activity_year").isNull()
            | (F.col("activity_year") != F.col("_ano").cast("string")),
            F.lit("activity_year diferente do arquivo"),
        )
    )

    c.texto("lei", "lei")
    c.texto("state_code", "estado")
    c.texto("county_code", "condado")
    c.texto("derived_msa_md", "msa")
    c.texto("census_tract", "setor_censitario")

    c.codigo("action_taken", "resultado")
    c.codigo("loan_purpose", "finalidade")
    c.codigo("loan_type", "tipo_emprestimo")
    c.codigo("lien_status", "prioridade_garantia")
    c.codigo("occupancy_type", "ocupacao")
    c.codigo("construction_method", "metodo_construcao")
    c.codigo("preapproval", "pre_aprovacao")
    c.codigo("purchaser_type", "comprador")
    c.codigo("hoepa_status", "hoepa")
    c.texto("total_units", "unidades", UNIDADES)
    c.texto("conforming_loan_limit", "limite_conforme", LIMITE_CONFORME)
    c.texto("derived_loan_product_type", "produto")
    c.texto("derived_dwelling_category", "categoria_moradia")
    c.sim_nao("reverse_mortgage", "hipoteca_reversa")
    c.sim_nao("open_end_line_of_credit", "linha_de_credito")
    c.sim_nao("business_or_commercial_purpose", "fim_comercial")
    c.sim_nao("negative_amortization", "amortizacao_negativa")
    c.sim_nao("interest_only_payment", "so_juros")
    c.sim_nao("balloon_payment", "parcela_balao")
    c.sim_nao("other_nonamortizing_features", "outras_nao_amortizantes")

    c.numero("loan_amount", "valor_emprestimo", "decimal(14,2)")
    c.numero("property_value", "valor_imovel", "decimal(14,2)")
    c.numero("income", "renda_milhares", "int")
    c.numero("combined_loan_to_value_ratio", "ltv", "decimal(12,3)")
    c.numero("interest_rate", "taxa_juros", "decimal(8,3)")
    c.numero("rate_spread", "spread_taxa", "decimal(8,3)")
    c.numero("total_loan_costs", "custos_totais", "decimal(14,2)")
    c.numero("total_points_and_fees", "pontos_e_taxas", "decimal(14,2)")
    c.numero("origination_charges", "encargos_originacao", "decimal(14,2)")
    c.numero("discount_points", "pontos_desconto", "decimal(14,2)")
    c.numero("lender_credits", "creditos_credor", "decimal(14,2)")
    c.numero("loan_term", "prazo_meses", "int")
    c.numero("prepayment_penalty_term", "prazo_multa_meses", "int")
    c.numero("intro_rate_period", "periodo_taxa_inicial_meses", "int")
    c.numero("multifamily_affordable_units", "unidades_acessiveis_pct", "int")

    dti = c.limpo("debt_to_income_ratio")
    c.rejeicoes.append(
        F.when(
            dti.isNotNull() & ~dti.isin(*FAIXAS_DTI) & ~dti.try_cast("int").between(36, 49),
            F.lit("debt_to_income_ratio fora do domínio"),
        )
    )
    c.colunas += [dti.alias("dti_faixa"), _grupo_dti(dti).alias("dti_grupo")]

    idade = c.limpo("applicant_age")
    c.rejeicoes.append(
        F.when(
            idade.isNotNull() & ~idade.isin(*FAIXAS_IDADE, "8888"),
            F.lit("applicant_age fora do domínio"),
        )
    )
    c.colunas.append(F.when(idade == "8888", F.lit(None)).otherwise(idade).alias("idade_faixa"))
    c.texto("derived_race", "raca")
    c.texto("derived_ethnicity", "etnia")
    c.texto("derived_sex", "sexo")
    c.codigo("applicant_sex", "sexo_solicitante")
    c.codigo("co_applicant_sex", "sexo_cosolicitante")
    c.codigo("applicant_credit_score_type", "tipo_score")
    for i in range(1, 5):
        c.codigo(f"denial_reason_{i}", f"motivo_negativa_{i}")

    c.numero("tract_population", "populacao_setor", "int")
    c.numero("tract_minority_population_percent", "pct_minoria_setor", "decimal(6,2)")
    c.numero("ffiec_msa_md_median_family_income", "renda_mediana_msa", "int")
    c.numero("tract_to_msa_income_percentage", "pct_renda_setor_msa", "decimal(8,2)")

    colunas = [*c.colunas]
    colunas.append(F.array_compact(F.array(*c.rejeicoes)).alias("_rejeicoes"))
    colunas.append(F.coalesce(F.greatest(*c.isencoes), F.lit(False)).alias("tem_isencao"))
    # O texto original vai junto num struct: só a tabela de rejeitados o guarda.
    brutas = [n for n in bronze.columns if n not in ("_ano", "_versao")]
    colunas.append(F.struct(*brutas).alias("_bruto"))
    df = bronze.select(*colunas)
    df = df.withColumn("tem_cosolicitante", F.col("sexo_cosolicitante") != 5)
    df = df.withColumn(
        "alertas",
        F.array_compact(
            F.array(
                F.when(
                    F.col("motivo_negativa_1").isNotNull() & ~F.col("resultado").isin(3, 7),
                    F.lit("motivo de negativa sem negativa"),
                ),
                # Juros só são informados para originado (1), aprovado (2) e comprado (6).
                F.when(
                    F.col("taxa_juros").isNotNull() & ~F.col("resultado").isin(1, 2, 6),
                    F.lit("juros em pedido sem aprovação"),
                ),
                F.when(F.col("estado").isNull(), F.lit("sem estado")),
            )
        ),
    )
    return df


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
    df = transformar(bronze).withColumn("congelado_em", F.lit(congelado_em(fonte)))
    df = df.persist()
    try:
        total = df.count()
        if total == 0:
            raise ValueError(f"{fonte.chave}: não está na bronze (rode hmda.bronze antes)")
        validas = df.where(F.size("_rejeicoes") == 0).drop("_rejeicoes", "_bruto")
        rejeitadas = df.where(F.size("_rejeicoes") > 0).select(
            "ano", "versao", "_rejeicoes", "_bruto"
        )

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

        caminho_rej = tabelas.caminho(raiz, tabelas.REJEITADOS)
        escrita_rej = rejeitadas.write.format("delta").partitionBy("ano", "versao")
        if DeltaTable.isDeltaTable(spark, caminho_rej):
            filtro_rej = f"ano = {fonte.ano} AND versao = '{fonte.versao}'"
            escrita_rej.mode("overwrite").option("replaceWhere", filtro_rej).save(caminho_rej)
        else:
            escrita_rej.mode("errorifexists").save(caminho_rej)
        n_rejeitadas = rejeitadas.count()
        n_alerta = validas.where(F.size("alertas") > 0).count()
    finally:
        df.unpersist()

    registro = {
        "ano": fonte.ano,
        "versao": fonte.versao,
        "congelado_em": congelado_em(fonte),
        "versao_delta": versao_delta,
        "linhas": total - n_rejeitadas,
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
