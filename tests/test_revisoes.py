"""Testes das revisões: multiconjunto, campo corrigido e prova contra o exceptAll."""

import pytest

from fabrica import linha, montar_zip
from hmda import bronze, revisoes, tabelas
from hmda.fontes import Fonte


@pytest.fixture
def lake(tmp_path):
    return tmp_path / "tabelas", tmp_path / "tmp"


def carregar(spark, tmp_path, lake, versao, linhas):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, linhas, nome=f"{versao}.zip")
    bronze.carregar(spark, Fonte(2021, versao), zp, sha, raiz, tmp)


A = linha(lei="A")
B = linha(lei="B")
C = linha(lei="C")


def test_diferenca_respeita_linhas_repetidas(spark, tmp_path, lake):
    raiz, _ = lake
    carregar(spark, tmp_path, lake, "snapshot", [A, A, B])
    carregar(spark, tmp_path, lake, "one_year", [A, B, B, C])
    r = revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year", estado_conferencia="DE")

    assert (r["antes"], r["depois"]) == (3, 4)
    assert r["iguais"] == 2  # um A e um B continuam
    assert (r["sairam"], r["entraram"]) == (1, 2)  # saiu um A; entraram um B e um C
    assert r["conferencia"] == {
        "exato_saiu": 1,
        "exato_entrou": 2,
        "hash_saiu": 1,
        "hash_entrou": 2,
    }

    inst = {
        x["lei"]: (x["antes"], x["depois"], x["entraram"], x["sairam"])
        for x in spark.read.format("delta")
        .load(tabelas.caminho(raiz, tabelas.REV_INSTITUICOES))
        .collect()
    }
    assert inst == {"A": (2, 1, 0, 1), "B": (1, 2, 1, 0), "C": (0, 1, 1, 0)}


def test_nulo_e_texto_vazio_nao_colidem(spark, tmp_path, lake):
    """("x", nulo) e (nulo, "x") são linhas diferentes: a marca de nulo evita a colisão."""
    raiz, _ = lake
    carregar(spark, tmp_path, lake, "snapshot", [linha(lei="L", county_code="", census_tract="X")])
    carregar(spark, tmp_path, lake, "one_year", [linha(lei="L", county_code="X", census_tract="")])
    r = revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year")
    assert (r["sairam"], r["entraram"]) == (1, 1)


def test_descobre_qual_campo_foi_corrigido(spark, tmp_path, lake):
    raiz, _ = lake
    base = dict(lei="BANCO1", census_tract="10003010100")
    carregar(
        spark,
        tmp_path,
        lake,
        "snapshot",
        [linha(**base, income="85"), linha(**base, income="85", loan_amount="105000.0"), A],
    )
    carregar(
        spark,
        tmp_path,
        lake,
        "one_year",
        # renda corrigida na 1ª; valor corrigido na 2ª; A continua igual
        [linha(**base, income="95"), linha(**base, income="85", loan_amount="155000.0"), A],
    )
    revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year")
    campos = {
        x["campo"]: x["pares"]
        for x in spark.read.format("delta")
        .load(tabelas.caminho(raiz, tabelas.REV_CAMPOS))
        .collect()
    }
    assert campos == {"income": 1, "loan_amount": 1}


def test_rodar_de_novo_substitui_o_resultado(spark, tmp_path, lake):
    raiz, _ = lake
    carregar(spark, tmp_path, lake, "snapshot", [A])
    carregar(spark, tmp_path, lake, "one_year", [B])
    revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year")
    revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year")
    dif = spark.read.format("delta").load(tabelas.caminho(raiz, tabelas.REV_DIFERENCAS))
    assert dif.count() == 2  # A saiu, B entrou; sem duplicar na 2ª execução


def test_versao_inventada_e_recusada(spark, lake):
    raiz, _ = lake
    with pytest.raises(ValueError, match="desconhecida"):
        revisoes.comparar(spark, raiz, 2021, "snapshot", "x' OR 1=1 --")


def test_formato_diferente_do_mesmo_numero_nao_e_revisao(spark, tmp_path, lake):
    """Achado real (2021): o One Year grava 2560.0, o Three Year grava 2560.00 ou 02560."""
    raiz, _ = lake
    carregar(
        spark,
        tmp_path,
        lake,
        "snapshot",
        [
            linha(
                lei="F",
                discount_points="2560.0",
                interest_rate="2.6499999999999999",
                loan_term="120",
            )
        ],
    )
    carregar(
        spark,
        tmp_path,
        lake,
        "one_year",
        [linha(lei="F", discount_points="2560.00", interest_rate="2.650", loan_term="00120")],
    )
    r = revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year")
    assert (r["sairam"], r["entraram"], r["iguais"]) == (0, 0, 1)


def test_campo_calculado_pelo_ffiec_nao_e_revisao_do_banco(spark, tmp_path, lake):
    """Achado real: o FFIEC recalculou os dados do censo (tract_*) no Three Year."""
    raiz, _ = lake
    carregar(
        spark, tmp_path, lake, "snapshot", [linha(lei="C", tract_to_msa_income_percentage="111.0")]
    )
    carregar(
        spark, tmp_path, lake, "one_year", [linha(lei="C", tract_to_msa_income_percentage="92.09")]
    )
    r = revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year")
    assert (r["sairam"], r["entraram"]) == (0, 0)


def test_mudanca_de_valor_de_verdade_continua_aparecendo(spark, tmp_path, lake):
    raiz, _ = lake
    carregar(spark, tmp_path, lake, "snapshot", [linha(lei="V", interest_rate="2.65")])
    carregar(spark, tmp_path, lake, "one_year", [linha(lei="V", interest_rate="2.75")])
    r = revisoes.comparar(spark, raiz, 2021, "snapshot", "one_year")
    assert (r["sairam"], r["entraram"]) == (1, 1)
