"""Testes da qualidade: conferência oficial célula a célula e bloqueio das etapas seguintes."""

import json

import pytest

from fabrica import linha, montar_zip
from hmda import bronze, dq, silver
from hmda.fontes import Fonte

SNAP = Fonte(2021, "snapshot")


@pytest.fixture
def lake(tmp_path):
    return tmp_path / "tabelas", tmp_path / "tmp"


def preparar(spark, tmp_path, lake, linhas):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, linhas)
    bronze.carregar(spark, SNAP, zp, sha, raiz, tmp)
    silver.aplicar(spark, SNAP, raiz)


def oficial(tmp_path, por_estado):
    pasta = tmp_path / "oficial"
    pasta.mkdir(exist_ok=True)
    conteudo = {
        "ano": 2021,
        "por_estado": {
            e: {a: {"contagem": n, "soma_valor": 0} for a, n in acoes.items()}
            for e, acoes in por_estado.items()
        },
    }
    (pasta / "contagens_2021.json").write_text(json.dumps(conteudo), encoding="utf-8")
    return pasta


LINHAS = [linha(), linha(), linha(action_taken="3", denial_reason_1="1"), linha(state_code="NA")]


def test_tudo_certo_aprova(spark, tmp_path, lake):
    raiz, _ = lake
    preparar(spark, tmp_path, lake, LINHAS)
    # A API não conta a linha sem estado (como na vida real).
    pasta = oficial(tmp_path, {"DE": {"1": 2, "3": 1}, "RI": {"1": 0}})
    resultados = dq.verificar(spark, raiz, pasta)
    assert all(r["passou"] for r in resultados), [r for r in resultados if not r["passou"]]
    assert {r["checagem"] for r in resultados} == {
        "restricoes",
        "versao_unica",
        "conservacao",
        "sem_rejeicao",
        "oficial",
        "essenciais",
    }
    dq.exigir_aprovacao(spark, raiz)  # não levanta


def test_diferenca_com_o_oficial_reprova_e_bloqueia(spark, tmp_path, lake):
    raiz, _ = lake
    preparar(spark, tmp_path, lake, LINHAS)
    pasta = oficial(tmp_path, {"DE": {"1": 3, "3": 1}})  # o oficial diz 3 originados
    resultados = dq.verificar(spark, raiz, pasta)
    falha = next(r for r in resultados if r["checagem"] == "oficial")
    assert not falha["passou"]
    assert "('DE', '1'), -1" in falha["detalhe"]
    with pytest.raises(dq.DadoReprovado, match="oficial"):
        dq.exigir_aprovacao(spark, raiz)


def test_linha_rejeitada_reprova(spark, tmp_path, lake):
    raiz, _ = lake
    preparar(spark, tmp_path, lake, [*LINHAS, linha(action_taken="99")])
    pasta = oficial(tmp_path, {"DE": {"1": 2, "3": 1}})
    resultados = {r["checagem"]: r for r in dq.verificar(spark, raiz, pasta)}
    assert resultados["conservacao"]["passou"]  # 5 = 4 + 1: nada sumiu
    assert not resultados["sem_rejeicao"]["passou"]


def test_sem_checagem_ou_silver_mais_nova_bloqueia(spark, tmp_path, lake):
    raiz, _ = lake
    preparar(spark, tmp_path, lake, LINHAS)
    with pytest.raises(dq.DadoReprovado, match="nenhuma checagem"):
        dq.exigir_aprovacao(spark, raiz)
    dq.verificar(spark, raiz, oficial(tmp_path, {"DE": {"1": 2, "3": 1}}))
    silver.aplicar(spark, SNAP, raiz, forcar=True)  # a silver muda depois da checagem
    with pytest.raises(dq.DadoReprovado, match="mudou depois"):
        dq.exigir_aprovacao(spark, raiz)
