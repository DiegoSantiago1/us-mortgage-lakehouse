"""Testes da gold com números calculados à mão."""

import pytest
from pyspark.sql import functions as F

from fabrica import linha, montar_zip
from hmda import bronze, dq, gold, silver, tabelas
from hmda.fontes import Fonte

SNAP = Fonte(2021, "snapshot")


@pytest.fixture
def lake(tmp_path):
    return tmp_path / "tabelas", tmp_path / "tmp"


def silver_de(spark, tmp_path, lake, linhas):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, linhas)
    bronze.carregar(spark, SNAP, zp, sha, raiz, tmp)
    silver.aplicar(spark, SNAP, raiz)
    return spark.read.format("delta").load(tabelas.caminho(raiz, tabelas.SILVER))


def test_preco_renda_usa_so_compras_originadas_comparaveis(spark, tmp_path, lake):
    s = silver_de(
        spark,
        tmp_path,
        lake,
        [
            linha(property_value="300000", income="100"),  # 3,0
            linha(property_value="500000", income="100"),  # 5,0
            linha(property_value="200000", income="50"),  # 4,0
            linha(property_value="900000", income="10", loan_purpose="31"),  # refinanciamento
            linha(property_value="900000", income="10", action_taken="3"),  # negado
            linha(property_value="900000", income="Exempt"),  # sem renda
            linha(property_value="900000", income="10", occupancy_type="3"),  # investimento
            linha(property_value="900000", income="10", total_units="5-24"),  # multifamiliar
        ],
    )
    r = gold.preco_renda(s).where(F.col("nivel") == "nacional").collect()[0]
    assert r["compras"] == 3
    assert float(r["preco_renda"]) == pytest.approx(4.0)
    assert float(r["valor_imovel_mediano"]) == 300000
    assert float(r["renda_mediana"]) == 100000
    estado = gold.preco_renda(s).where(F.col("nivel") == "estado").collect()[0]
    assert (estado["lugar"], estado["compras"]) == ("DE", 3)


def test_msa_pequena_nao_entra_no_preco_renda(spark, tmp_path, lake):
    s = silver_de(spark, tmp_path, lake, [linha()] * 3)
    assert gold.preco_renda(s).where(F.col("nivel") == "msa").count() == 0


def test_taxa_de_negativa_e_motivos(spark, tmp_path, lake):
    s = silver_de(
        spark,
        tmp_path,
        lake,
        [
            linha(),
            linha(),
            linha(action_taken="2"),
            linha(action_taken="3", denial_reason_1="1", denial_reason_2="3", interest_rate="NA"),
            linha(action_taken="4"),  # desistência: fora do denominador
            linha(action_taken="3", loan_purpose="31", denial_reason_1="4", interest_rate="NA"),
        ],
    )
    neg = {
        r["finalidade_nome"]: (r["pedidos"], r["negados"], r["taxa_negativa"])
        for r in gold.negativas(s).where(F.col("nivel") == "nacional").collect()
    }
    assert neg == {"compra": (4, 1, 0.25), "refinanciamento": (1, 1, 1.0)}

    mot = {
        (r["finalidade_nome"], r["motivo_nome"]): r["pct_dos_negados"]
        for r in gold.motivos(s).collect()
    }
    assert mot == {
        ("compra", "dívida/renda (DTI)"): 1.0,
        ("compra", "histórico de crédito"): 1.0,
        ("refinanciamento", "garantia (imóvel)"): 1.0,
    }


def test_equidade_compara_com_quem_tem_o_mesmo_perfil(spark, tmp_path, lake):
    grupo_a = dict(derived_race="White", derived_ethnicity="Not Hispanic or Latino")
    grupo_b = dict(
        derived_race="Black or African American", derived_ethnicity="Not Hispanic or Latino"
    )
    negado = dict(action_taken="3", denial_reason_1="1", interest_rate="NA")
    linhas = (
        [linha(**grupo_a, **negado)] * 6
        + [linha(**grupo_a)] * 24
        + [linha(**grupo_b, **negado)] * 12
        + [linha(**grupo_b)] * 18
        # Perfil diferente (renda alta) e célula pequena: fica fora da comparação controlada.
        + [linha(**grupo_b, income="400")] * 5
    )
    s = silver_de(spark, tmp_path, lake, linhas)
    eq = {
        r["grupo"]: r for r in gold.equidade(s).where(F.col("dimensao") == "raça/etnia").collect()
    }
    branco, negro = eq["Branco não hispânico"], eq["Negro"]
    assert (branco["pedidos"], branco["negados"]) == (30, 6)
    assert branco["esperados"] == pytest.approx(9.0)  # 30 × 18/60
    assert branco["razao_obs_esp"] == pytest.approx(6 / 9)
    assert negro["razao_obs_esp"] == pytest.approx(12 / 9)
    assert negro["taxa_bruta"] == pytest.approx(12 / 35)  # a bruta inclui os 5 de fora
    assert negro["taxa_ajustada"] == pytest.approx(12 / 9 * 0.3)
    assert branco["pedidos_comparaveis"] == 60
    assert not negro["publicavel"]  # menos de 1.000 pedidos: não publicar


def test_gold_se_recusa_sem_qualidade_aprovada(spark, tmp_path, lake):
    raiz, _ = lake
    silver_de(spark, tmp_path, lake, [linha()])
    with pytest.raises(dq.DadoReprovado):
        gold.construir(spark, raiz)


def test_renda_gigante_nao_estoura(spark, tmp_path, lake):
    """Achado real: renda informada acima de US$ 2,1 bi estourava o int ao multiplicar por 1000."""
    s = silver_de(spark, tmp_path, lake, [linha(income="9999999"), linha(), linha()])
    r = gold.preco_renda(s).where(F.col("nivel") == "nacional").collect()[0]
    assert r["compras"] == 3
