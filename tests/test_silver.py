"""Testes da silver: tipos, rejeições, alertas, troca de versão e CHECK constraints."""

from decimal import Decimal

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from fabrica import linha, montar_zip
from hmda import bronze, silver, tabelas
from hmda.fontes import Fonte


@pytest.fixture
def lake(tmp_path):
    return tmp_path / "tabelas", tmp_path / "tmp"


def carregar_bronze(spark, tmp_path, lake, fonte, linhas, nome=None):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, linhas, nome=nome or f"{fonte.ano}_{fonte.versao}.zip")
    bronze.carregar(spark, fonte, zp, sha, raiz, tmp)


def ler(spark, raiz, nome):
    return spark.read.format("delta").load(tabelas.caminho(raiz, nome))


SNAP = Fonte(2021, "snapshot")


def test_tipos_e_valores_especiais_viram_nulo_nunca_zero(spark, tmp_path, lake):
    raiz, _ = lake
    carregar_bronze(
        spark,
        tmp_path,
        lake,
        SNAP,
        [
            linha(lei="NORMAL"),
            linha(lei="ISENTO", income="Exempt", property_value="Exempt", reverse_mortgage="1111"),
            linha(
                lei="SEMVALOR", interest_rate="NA", debt_to_income_ratio="NA", applicant_age="8888"
            ),
        ],
    )
    reg = silver.aplicar(spark, SNAP, raiz)
    assert reg["linhas"] == 3 and reg["rejeitadas"] == 0

    linhas = {r["lei"]: r for r in ler(spark, raiz, tabelas.SILVER).collect()}
    normal = linhas["NORMAL"]
    assert normal["valor_emprestimo"] == Decimal("205000.00")
    assert normal["renda_milhares"] == 85
    assert normal["taxa_juros"] == Decimal("3.125")
    assert normal["resultado"] == 1 and normal["finalidade"] == 1
    assert normal["hipoteca_reversa"] is False
    assert normal["motivo_negativa_1"] is None  # 10 = "não se aplica"
    assert normal["tem_cosolicitante"] is False  # 5 = sem cosolicitante
    assert normal["tem_isencao"] is False
    assert normal["dti_grupo"] == "20-30%"

    isento = linhas["ISENTO"]
    assert isento["renda_milhares"] is None and isento["valor_imovel"] is None
    assert isento["hipoteca_reversa"] is None
    assert isento["tem_isencao"] is True

    na = linhas["SEMVALOR"]
    assert na["taxa_juros"] is None and na["dti_faixa"] is None and na["idade_faixa"] is None


@pytest.mark.parametrize(
    ("bruto", "grupo"),
    [("<20%", "<20%"), ("36", "36-40%"), ("44", "40-45%"), ("49", "45-50%"), (">60%", ">60%")],
)
def test_grupos_de_dti(spark, tmp_path, lake, bruto, grupo):
    raiz, _ = lake
    carregar_bronze(spark, tmp_path, lake, SNAP, [linha(debt_to_income_ratio=bruto)])
    silver.aplicar(spark, SNAP, raiz)
    assert ler(spark, raiz, tabelas.SILVER).collect()[0]["dti_grupo"] == grupo


def test_linhas_invalidas_vao_para_rejeitados_com_o_motivo(spark, tmp_path, lake):
    raiz, _ = lake
    carregar_bronze(
        spark,
        tmp_path,
        lake,
        SNAP,
        [
            linha(lei="BOA"),
            linha(lei="CODIGO", action_taken="9"),
            linha(lei="NUMERO", loan_amount="duzentos mil"),
            linha(lei="ANO", activity_year="2020"),
            linha(lei="DTI", debt_to_income_ratio="35"),  # 35 não existe: é "30%-<36%"
            linha() + ",coluna_a_mais",
        ],
    )
    reg = silver.aplicar(spark, SNAP, raiz)
    assert (reg["linhas"], reg["rejeitadas"]) == (1, 5)
    assert [r["lei"] for r in ler(spark, raiz, tabelas.SILVER).collect()] == ["BOA"]

    motivos = {
        r["_bruto"]["lei"]: r["_rejeicoes"] for r in ler(spark, raiz, tabelas.REJEITADOS).collect()
    }
    assert motivos["CODIGO"] == ["action_taken fora do domínio"]
    assert motivos["NUMERO"] == ["loan_amount não numérico"]
    assert motivos["ANO"] == ["activity_year diferente do arquivo"]
    assert motivos["DTI"] == ["debt_to_income_ratio fora do domínio"]
    assert "linha malformada" in motivos["5493001KJTIIGC8Y1R12"]


def test_incoerencia_de_negocio_vira_alerta_e_a_linha_fica(spark, tmp_path, lake):
    raiz, _ = lake
    carregar_bronze(
        spark,
        tmp_path,
        lake,
        SNAP,
        [
            linha(lei="NEGADA", action_taken="3", denial_reason_1="1", interest_rate="NA"),
            linha(lei="ESTRANHA", action_taken="1", denial_reason_1="3"),
            linha(lei="SEM_ESTADO", state_code="NA"),
        ],
    )
    reg = silver.aplicar(spark, SNAP, raiz)
    assert (reg["linhas"], reg["rejeitadas"], reg["com_alerta"]) == (3, 0, 2)
    alertas = {r["lei"]: r["alertas"] for r in ler(spark, raiz, tabelas.SILVER).collect()}
    assert alertas == {
        "NEGADA": [],
        "ESTRANHA": ["motivo de negativa sem negativa"],
        "SEM_ESTADO": ["sem estado"],
    }


def test_versao_nova_substitui_o_ano_e_a_antiga_fica_no_time_travel(spark, tmp_path, lake):
    raiz, _ = lake
    um_ano = Fonte(2021, "one_year")
    outro_ano = Fonte(2022, "snapshot")
    carregar_bronze(spark, tmp_path, lake, SNAP, [linha(lei="S")] * 2)
    carregar_bronze(spark, tmp_path, lake, um_ano, [linha(lei="O")] * 3)
    carregar_bronze(spark, tmp_path, lake, outro_ano, [linha(lei="X", activity_year="2022")])

    v_snap = silver.aplicar(spark, SNAP, raiz)["versao_delta"]
    silver.aplicar(spark, outro_ano, raiz)
    v_one = silver.aplicar(spark, um_ano, raiz)["versao_delta"]

    atual = ler(spark, raiz, tabelas.SILVER).groupBy("ano", "versao", "lei").count().collect()
    assert {(r["ano"], r["versao"], r["lei"], r["count"]) for r in atual} == {
        (2021, "one_year", "O", 3),
        (2022, "snapshot", "X", 1),
    }
    caminho = tabelas.caminho(raiz, tabelas.SILVER)
    antes = spark.read.format("delta").option("versionAsOf", v_snap).load(caminho)
    assert {r["lei"] for r in antes.collect()} == {"S"}
    assert v_one > v_snap

    versoes = ler(spark, raiz, tabelas.SILVER_VERSOES).orderBy("aplicado_em").collect()
    assert [(r["ano"], r["versao"]) for r in versoes] == [
        (2021, "snapshot"),
        (2022, "snapshot"),
        (2021, "one_year"),
    ]


def test_versao_menos_madura_ou_repetida_nao_faz_nada(spark, tmp_path, lake):
    raiz, _ = lake
    um_ano = Fonte(2021, "one_year")
    carregar_bronze(spark, tmp_path, lake, SNAP, [linha(lei="S")])
    carregar_bronze(spark, tmp_path, lake, um_ano, [linha(lei="O")])
    silver.aplicar(spark, um_ano, raiz)
    caminho = tabelas.caminho(raiz, tabelas.SILVER)
    versao = DeltaTable.forPath(spark, caminho).history(1).collect()[0]["version"]

    silver.aplicar(spark, SNAP, raiz)  # mais antiga: ignorada
    silver.aplicar(spark, um_ano, raiz)  # repetida: ignorada
    assert DeltaTable.forPath(spark, caminho).history(1).collect()[0]["version"] == versao
    assert {r["lei"] for r in ler(spark, raiz, tabelas.SILVER).collect()} == {"O"}


def test_check_constraint_barra_escrita_direta_fora_do_dominio(spark, tmp_path, lake):
    """Defesa em camadas: mesmo sem passar pelo silver.py, a tabela recusa lixo."""
    raiz, _ = lake
    carregar_bronze(spark, tmp_path, lake, SNAP, [linha()])
    silver.aplicar(spark, SNAP, raiz)
    caminho = tabelas.caminho(raiz, tabelas.SILVER)
    tabela = ler(spark, raiz, tabelas.SILVER)

    for coluna, valor in [("resultado", 9), ("finalidade", 3), ("dti_faixa", "35")]:
        hostil = tabela.limit(1).withColumn(
            coluna, F.lit(valor).cast(tabela.schema[coluna].dataType)
        )
        with pytest.raises(Exception, match=r"(?i)check constraint"):
            hostil.write.format("delta").mode("append").save(caminho)
    assert ler(spark, raiz, tabelas.SILVER).count() == 1


def test_ano_ausente_na_bronze_e_erro(spark, lake):
    raiz, _ = lake
    with pytest.raises(Exception):  # noqa: B017 (sem bronze nenhuma: o Delta reclama do caminho)
        silver.aplicar(spark, SNAP, raiz)


def test_1111_e_isencao_so_nos_campos_codificados(spark, tmp_path, lake):
    """1111 = "isento" no tipo de score (código); na renda, 1111 é US$ 1,111 milhão."""
    raiz, _ = lake
    carregar_bronze(
        spark,
        tmp_path,
        lake,
        SNAP,
        [linha(lei="SCORE", applicant_credit_score_type="1111"), linha(lei="RICO", income="1111")],
    )
    reg = silver.aplicar(spark, SNAP, raiz)
    assert reg["rejeitadas"] == 0
    linhas = {r["lei"]: r for r in ler(spark, raiz, tabelas.SILVER).collect()}
    assert linhas["SCORE"]["tipo_score"] is None and linhas["SCORE"]["tem_isencao"] is True
    assert linhas["RICO"]["renda_milhares"] == 1111 and linhas["RICO"]["tem_isencao"] is False


def test_casos_reais_de_2018_nao_sao_rejeitados(spark, tmp_path, lake):
    """Achados na carga real: 1111 nos motivos (isento), juros absurdos e idade 9999."""
    raiz, _ = lake
    carregar_bronze(
        spark,
        tmp_path,
        lake,
        SNAP,
        [
            linha(lei="ISENTO", denial_reason_1="1111", debt_to_income_ratio="Exempt"),
            linha(lei="JUROS", interest_rate="260000.0"),
            linha(lei="SPREAD", interest_rate="Exempt", rate_spread="-9999997.0"),
            linha(lei="IDADE", applicant_age="9999"),
        ],
    )
    reg = silver.aplicar(spark, SNAP, raiz)
    assert (reg["linhas"], reg["rejeitadas"]) == (4, 0)
    linhas = {r["lei"]: r for r in ler(spark, raiz, tabelas.SILVER).collect()}
    assert linhas["ISENTO"]["motivo_negativa_1"] is None and linhas["ISENTO"]["tem_isencao"]
    assert linhas["JUROS"]["alertas"] == ["juros implausível"]
    assert float(linhas["JUROS"]["taxa_juros"]) == 260000.0  # guardado como veio
    assert linhas["SPREAD"]["alertas"] == ["spread implausível"]
    assert linhas["IDADE"]["idade_faixa"] is None
    assert linhas["IDADE"]["alertas"] == ["idade 9999 fora da documentação"]


def test_codigos_novos_de_score_e_unidades_zero_nao_sao_rejeitados(spark, tmp_path, lake):
    """Achados na carga real: score 11-15 (2022+) e total_units = "0" (1 linha em 2019)."""
    raiz, _ = lake
    carregar_bronze(
        spark,
        tmp_path,
        lake,
        SNAP,
        [
            linha(lei="FICO10T", applicant_credit_score_type="14"),
            linha(lei="ZERO", total_units="0"),
        ],
    )
    reg = silver.aplicar(spark, SNAP, raiz)
    assert (reg["linhas"], reg["rejeitadas"]) == (2, 0)
    linhas = {r["lei"]: r for r in ler(spark, raiz, tabelas.SILVER).collect()}
    assert linhas["FICO10T"]["tipo_score"] == 14
    assert linhas["ZERO"]["unidades"] is None
    assert linhas["ZERO"]["alertas"] == ["total_units fora da documentação"]
