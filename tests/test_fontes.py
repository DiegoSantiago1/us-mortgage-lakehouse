import pytest

from hmda.fontes import Fonte


def test_urls_seguem_o_padrao_do_site_oficial():
    assert Fonte(2022, "snapshot").url == (
        "https://files.ffiec.cfpb.gov/static-data/snapshot/2022/2022_public_lar_csv.zip"
    )
    assert Fonte(2022, "one_year").url == (
        "https://files.ffiec.cfpb.gov/static-data/one-year/2022/2022_public_lar_one_year_csv.zip"
    )
    assert Fonte(2021, "three_year").url == (
        "https://files.ffiec.cfpb.gov/static-data/three-year/2021/"
        "2021_public_lar_three_year_csv.zip"
    )


def test_ordem_de_maturidade():
    assert [Fonte(2021, v).ordem for v in ("snapshot", "one_year", "three_year")] == [0, 1, 2]


@pytest.mark.parametrize(
    ("ano", "versao", "trecho"),
    [
        (2017, "three_year", "começa em 2018"),
        (2018, "one_year", "não publica"),  # o FFIEC não tem One Year de 2018
        (2023, "three_year", "não publica"),  # Three Year de 2023 ainda não saiu
        (2026, "snapshot", "não publica"),
        (2021, "final", "desconhecida"),
        (2021, "../../etc", "desconhecida"),
    ],
)
def test_recusa_ano_ou_versao_inexistente(ano, versao, trecho):
    with pytest.raises(ValueError, match=trecho):
        Fonte(ano, versao)


def test_escopo_tem_uma_vigente_por_ano_e_tres_versoes_de_2021_e_2022():
    from hmda.fontes import escopo

    chaves = [f.chave for f in escopo()]
    assert chaves == [
        "2018/three_year",
        "2019/three_year",
        "2020/three_year",
        "2021/snapshot",
        "2021/one_year",
        "2021/three_year",
        "2022/snapshot",
        "2022/one_year",
        "2022/three_year",
        "2023/one_year",
        "2024/one_year",
        "2025/snapshot",
    ]


def test_toda_fonte_do_escopo_tem_data_de_congelamento():
    from hmda.fontes import congelado_em, escopo

    for fonte in escopo():
        congelado_em(fonte)  # KeyError se faltar


def test_ordem_de_publicacao_segue_as_datas_do_ffiec():
    from hmda.fontes import escopo, ordem_de_publicacao

    ordem = [f.chave for f in ordem_de_publicacao(escopo())]
    assert ordem == [
        "2018/three_year",  # 31/12/2021
        "2021/snapshot",  # 30/04/2022
        "2019/three_year",  # 31/12/2022
        "2021/one_year",  # 01/05/2023 (empate: ano menor primeiro)
        "2022/snapshot",  # 01/05/2023
        "2020/three_year",  # 31/12/2023
        "2022/one_year",  # 01/05/2024
        "2021/three_year",  # 31/12/2024
        "2023/one_year",  # 19/05/2025
        "2022/three_year",  # 31/12/2025
        "2024/one_year",  # 02/06/2026
        "2025/snapshot",  # 02/06/2026
    ]
    # Dentro de um mesmo ano, a maturidade nunca anda para trás.
    for ano in (2021, 2022):
        versoes = [f.ordem for f in ordem_de_publicacao(escopo()) if f.ano == ano]
        assert versoes == sorted(versoes)
