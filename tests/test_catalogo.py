"""Catálogo de tabelas: registro idempotente, poda de partições medida e o plano do EXPLAIN.

Os testes usam o catálogo em memória da sessão compartilhada (o metastore Hive com Derby
é a mesma API, só que persistente; ele roda no `python -m hmda.catalogo`).
"""

import pytest

from hmda import catalogo


@pytest.fixture
def lake(spark, tmp_path):
    """Uma silver mínima particionada por ano e uma segunda tabela, em outra camada."""
    raiz = tmp_path / "tabelas"
    linhas = [(2021, "TX"), (2021, "CA"), (2022, "TX"), (2022, "CA"), (2022, "NY"), (2023, "TX")]
    df = spark.createDataFrame(linhas, "ano INT, estado STRING")
    # Um arquivo por ano: a contagem de arquivos lidos fica previsível.
    df.repartition("ano").write.format("delta").partitionBy("ano").save(
        str(raiz / "silver" / "vigente")
    )
    spark.createDataFrame([("x", 1)], "campo STRING, n INT").write.format("delta").save(
        str(raiz / "revisoes" / "campos")
    )
    yield raiz
    for esquema in ("silver", "revisoes"):
        spark.sql(f"DROP SCHEMA IF EXISTS {esquema} CASCADE")  # externas: os dados ficam


def test_nome_no_catalogo():
    assert catalogo.nome_no_catalogo("silver/vigente") == ("silver", "vigente")
    for ruim in ("silver", "a/b/c", "Silver/vigente", "silver/vig-ente", "x/1; DROP TABLE y"):
        with pytest.raises(ValueError):
            catalogo.nome_no_catalogo(ruim)


def test_encontra_so_pastas_com_tabela_delta(lake):
    (lake / "silver" / "sem_delta").mkdir()  # pasta sem _delta_log não é tabela
    assert catalogo.tabelas_existentes(lake) == ["revisoes/campos", "silver/vigente"]


def test_registra_como_tabela_externa_e_e_idempotente(spark, lake):
    assert catalogo.registrar(spark, lake) == ["revisoes.campos", "silver.vigente"]
    assert catalogo.registrar(spark, lake) == ["revisoes.campos", "silver.vigente"]
    assert spark.sql("SELECT COUNT(*) FROM silver.vigente").first()[0] == 6
    # Tirar do catálogo não apaga os dados (tabela externa).
    spark.sql("DROP TABLE silver.vigente")
    assert spark.read.format("delta").load(str(lake / "silver" / "vigente")).count() == 6


def test_local_mudou_o_registro_e_refeito(spark, lake, tmp_path):
    catalogo.registrar(spark, lake)
    outra = tmp_path / "outra" / "tabelas"
    spark.createDataFrame([(2024, "FL")], "ano INT, estado STRING").write.format(
        "delta"
    ).partitionBy("ano").save(str(outra / "silver" / "vigente"))
    catalogo.registrar(spark, outra)
    assert spark.sql("SELECT estado FROM silver.vigente").first()[0] == "FL"


def test_filtro_na_particao_le_so_a_pasta_do_ano(spark, lake):
    catalogo.registrar(spark, lake)
    tudo = catalogo.leitura(spark, "SELECT * FROM silver.vigente")
    so_2022 = catalogo.leitura(spark, "SELECT * FROM silver.vigente WHERE ano = 2022")
    assert tudo.pastas == ("ano=2021", "ano=2022", "ano=2023")
    assert so_2022.pastas == ("ano=2022",)
    assert so_2022.arquivos < tudo.arquivos


def test_explain_mostra_o_filtro_de_particao(spark, lake):
    catalogo.registrar(spark, lake)
    com = catalogo.explicar(spark, "SELECT * FROM silver.vigente WHERE ano = 2022")
    sem = catalogo.explicar(spark, "SELECT * FROM silver.vigente WHERE estado = 'TX'")
    [filtro] = catalogo.filtros_de_particao(com)
    assert "ano" in filtro and "2022" in filtro
    # Filtro em coluna comum não poda pasta nenhuma: o plano nem traz filtro de partição.
    assert catalogo.filtros_de_particao(sem) == []
    assert "estado" in sem  # o filtro existe, mas é aplicado depois de ler os arquivos


def test_relatorio_tem_tabelas_e_medicoes(spark, lake, tmp_path):
    catalogo.registrar(spark, lake)
    texto = catalogo.relatorio(spark, tmp_path / "CATALOGO.md")
    assert "`silver.vigente`" in texto and "`revisoes.campos`" in texto
    assert "| filtro na partição (ano = 2022) | " in texto
    assert "1: ano=2022" in texto
    assert (tmp_path / "CATALOGO.md").read_text(encoding="utf-8") == texto


# ---------------------------------------------------------------- amostra para o Databricks
def test_amostra_mesma_fracao_por_ano_e_sem_colunas_internas(spark):
    from hmda.amostra import amostrar

    linhas = [(ano, f"E{i % 5}", "x") for ano in (2021, 2022) for i in range(2000)]
    silver = spark.createDataFrame(linhas, "ano INT, estado STRING, _bruto STRING")
    a = amostrar(silver, 0.1)
    assert "_bruto" not in a.columns
    por_ano = {r["ano"]: r["count"] for r in a.groupBy("ano").count().collect()}
    assert set(por_ano) == {2021, 2022}
    assert all(150 <= n <= 250 for n in por_ano.values())  # ~10% de 2000
    # Semente fixa: rodar de novo dá exatamente a mesma amostra.
    assert sorted(a.collect()) == sorted(amostrar(silver, 0.1).collect())
    with pytest.raises(ValueError):
        amostrar(silver, 0)
