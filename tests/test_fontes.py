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
