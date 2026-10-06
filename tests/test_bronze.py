"""Testes da bronze com zips pequenos montados à mão (mesmo cabeçalho do FFIEC)."""

import hashlib
import zipfile
from pathlib import Path

import pytest
from delta.tables import DeltaTable

from hmda import bronze, tabelas
from hmda.esquema import COLUNAS_LAR
from hmda.fontes import Fonte

CABECALHO = ",".join(COLUNAS_LAR)


def linha(**valores: str) -> str:
    base = {c: "NA" for c in COLUNAS_LAR}
    base.update(activity_year="2021", lei="5493001KJTIIGC8Y1R12", state_code="DE", action_taken="1")
    base.update(valores)
    return ",".join(base[c] for c in COLUNAS_LAR)


def montar_zip(
    pasta: Path,
    linhas: list[str],
    nome: str = "lar.zip",
    cabecalho: str = CABECALHO,
    lixo_macos: bool = True,
    final_sem_quebra: bool = False,
) -> tuple[Path, str]:
    corpo = cabecalho + "\n" + "\n".join(linhas) + ("" if final_sem_quebra else "\n")
    caminho = pasta / nome
    with zipfile.ZipFile(caminho, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("2021_public_lar_csv.csv", corpo)
        if lixo_macos:
            z.writestr("__MACOSX/._2021_public_lar_csv.csv", b"\x00\x05\x16\x07lixo")
    return caminho, hashlib.sha256(caminho.read_bytes()).hexdigest()


@pytest.fixture
def lake(tmp_path):
    return tmp_path / "tabelas", tmp_path / "tmp"


def ler_bronze(spark, raiz):
    return spark.read.format("delta").load(tabelas.caminho(raiz, tabelas.BRONZE))


FONTE = Fonte(2021, "snapshot")


def test_carrega_tudo_como_texto_com_metadados(spark, tmp_path, lake):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, [linha(loan_amount="205000"), linha(income="Exempt")])
    reg = bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)

    assert reg["linhas_csv"] == reg["linhas_bronze"] == 2
    assert reg["corrompidas"] == 0
    df = ler_bronze(spark, raiz)
    assert {f.dataType.simpleString() for f in df.schema if f.name in COLUNAS_LAR} == {"string"}
    linhas = {r["loan_amount"]: r for r in df.collect()}
    assert linhas["205000"]["_ano"] == 2021
    assert linhas["205000"]["_versao"] == "snapshot"
    assert linhas["205000"]["_sha256_arquivo"] == sha
    assert linhas["NA"]["income"] == "Exempt"  # nada de converter na bronze
    assert not tmp.exists() or not any(tmp.iterdir())  # CSV temporário apagado


def test_duplicatas_legitimas_sao_mantidas(spark, tmp_path, lake):
    """Sem ULI, dois pedidos idênticos são possíveis: a bronze não deduplica."""
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, [linha(), linha(), linha()])
    assert bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)["linhas_bronze"] == 3


def test_rodar_de_novo_com_o_mesmo_arquivo_nao_faz_nada(spark, tmp_path, lake):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, [linha()])
    bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)
    caminho = tabelas.caminho(raiz, tabelas.BRONZE)
    versao = DeltaTable.forPath(spark, caminho).history(1).collect()[0]["version"]

    bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)
    assert DeltaTable.forPath(spark, caminho).history(1).collect()[0]["version"] == versao
    assert ler_bronze(spark, raiz).count() == 1


def test_arquivo_novo_substitui_so_a_sua_particao(spark, tmp_path, lake):
    raiz, tmp = lake
    zp1, sha1 = montar_zip(tmp_path, [linha(lei="A")] * 2, nome="snap.zip")
    zp2, sha2 = montar_zip(tmp_path, [linha(lei="B")] * 3, nome="one.zip")
    bronze.carregar(spark, FONTE, zp1, sha1, raiz, tmp)
    bronze.carregar(spark, Fonte(2021, "one_year"), zp2, sha2, raiz, tmp)

    # O FFIEC republica o Snapshot com uma linha a mais.
    zp3, sha3 = montar_zip(tmp_path, [linha(lei="C")] * 3, nome="snap_v2.zip")
    bronze.carregar(spark, FONTE, zp3, sha3, raiz, tmp)

    por_versao = {
        (r["_versao"], r["lei"]): r["count"]
        for r in ler_bronze(spark, raiz).groupBy("_versao", "lei").count().collect()
    }
    assert por_versao == {("snapshot", "C"): 3, ("one_year", "B"): 3}


def test_linha_malformada_fica_marcada_e_nao_some(spark, tmp_path, lake):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, [linha(), linha() + ",coluna_a_mais", "curta,demais"])
    reg = bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)
    assert reg["linhas_bronze"] == 3
    assert reg["corrompidas"] == 2
    ruins = ler_bronze(spark, raiz).where("_registro_corrompido IS NOT NULL").collect()
    assert {r["_registro_corrompido"].split(",")[-1] for r in ruins} == {"coluna_a_mais", "demais"}


def test_ultima_linha_sem_quebra_conta(spark, tmp_path, lake):
    raiz, tmp = lake
    zp, sha = montar_zip(tmp_path, [linha(), linha(lei="ultima")], final_sem_quebra=True)
    assert bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)["linhas_bronze"] == 2


def test_cabecalho_diferente_e_recusado_sem_gravar_nada(spark, tmp_path, lake):
    raiz, tmp = lake
    trocado = ",".join("renda" if c == "income" else c for c in COLUNAS_LAR)
    zp, sha = montar_zip(tmp_path, [linha()], cabecalho=trocado)
    with pytest.raises(bronze.ErroCarga, match=r"sobram \['renda'\], faltam \['income'\]"):
        bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)
    assert not DeltaTable.isDeltaTable(spark, tabelas.caminho(raiz, tabelas.BRONZE))


def test_colunas_na_ordem_errada_sao_recusadas(spark, tmp_path, lake):
    raiz, tmp = lake
    cols = list(COLUNAS_LAR)
    cols[0], cols[1] = cols[1], cols[0]
    zp, sha = montar_zip(tmp_path, [linha()], cabecalho=",".join(cols))
    with pytest.raises(bronze.ErroCarga, match="ordem diferente"):
        bronze.carregar(spark, FONTE, zp, sha, raiz, tmp)


def test_zip_corrompido_falha_e_limpa_o_temporario(spark, tmp_path, lake):
    raiz, tmp = lake
    zp, _ = montar_zip(tmp_path, [linha(lei=f"L{i}") for i in range(2000)])
    dados = bytearray(zp.read_bytes())
    meio = len(dados) // 2
    dados[meio : meio + 64] = bytes(64)  # estraga o conteúdo comprimido
    zp.write_bytes(bytes(dados))
    with pytest.raises(bronze.ErroCarga):
        bronze.carregar(spark, FONTE, zp, "x" * 64, raiz, tmp)
    assert not (tmp / "2021_snapshot").exists()


def test_zip_sem_csv_ou_com_dois_csv_e_recusado(spark, tmp_path, lake):
    raiz, tmp = lake
    zp = tmp_path / "dois.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("a.csv", CABECALHO + "\n")
        z.writestr("b.csv", CABECALHO + "\n")
    with pytest.raises(bronze.ErroCarga, match="esperava 1 CSV"):
        bronze.carregar(spark, FONTE, zp, "y" * 64, raiz, tmp)


def test_contagem_que_nao_bate_restaura_a_tabela(spark, tmp_path, lake, monkeypatch):
    """Se o gravado não bate com o CSV, a bronze volta exatamente ao que era."""
    raiz, tmp = lake
    zp1, sha1 = montar_zip(tmp_path, [linha(lei="BOA")] * 2, nome="v1.zip")
    bronze.carregar(spark, FONTE, zp1, sha1, raiz, tmp)

    original = bronze.extrair

    def extrair_mentindo(*args, **kwargs):
        ext = original(*args, **kwargs)
        return bronze.Extraido(ext.csv, ext.cabecalho, ext.linhas_de_dados + 1)

    monkeypatch.setattr(bronze, "extrair", extrair_mentindo)
    zp2, sha2 = montar_zip(tmp_path, [linha(lei="NOVA")] * 5, nome="v2.zip")
    with pytest.raises(bronze.ErroCarga, match="restaurada"):
        bronze.carregar(spark, FONTE, zp2, sha2, raiz, tmp)

    leis = {r["lei"] for r in ler_bronze(spark, raiz).collect()}
    assert leis == {"BOA"}
    assert bronze.ultima_carga(spark, raiz, FONTE)["sha256"] == sha1  # carga ruim não registrada
