"""Onde estão os arquivos nacionais do HMDA (LAR) de cada ano e versão.

Conferido em 06/10/2026 no código do site oficial (github.com/cfpb/hmda-frontend,
src/data-publication/constants/*-dataset*.jsx) e com HEAD em cada URL.
"""

from dataclasses import dataclass
from datetime import date

BASE = "https://files.ffiec.cfpb.gov/static-data"

# Ordem de maturidade: cada versão substitui a anterior do mesmo ano.
VERSOES = ("snapshot", "one_year", "three_year")

DISPONIVEIS: dict[str, frozenset[int]] = {
    "snapshot": frozenset(range(2017, 2026)),
    "one_year": frozenset(range(2019, 2025)),  # não há One Year de 2017 e 2018
    "three_year": frozenset(range(2017, 2023)),
}

# A regra de 2018 mudou os campos; 2017 tem outro formato (D8).
PRIMEIRO_ANO = 2018


@dataclass(frozen=True)
class Fonte:
    ano: int
    versao: str

    def __post_init__(self) -> None:
        if self.versao not in VERSOES:
            raise ValueError(f"versão desconhecida: {self.versao!r} (use {', '.join(VERSOES)})")
        if self.ano < PRIMEIRO_ANO:
            raise ValueError(f"o projeto começa em {PRIMEIRO_ANO} (D8); pedido: {self.ano}")
        if self.ano not in DISPONIVEIS[self.versao]:
            raise ValueError(f"o FFIEC não publica {self.versao} para {self.ano}")

    @property
    def url(self) -> str:
        if self.versao == "snapshot":
            return f"{BASE}/snapshot/{self.ano}/{self.ano}_public_lar_csv.zip"
        pasta = self.versao.replace("_", "-")
        return f"{BASE}/{pasta}/{self.ano}/{self.ano}_public_lar_{self.versao}_csv.zip"

    @property
    def chave(self) -> str:
        return f"{self.ano}/{self.versao}"

    @property
    def ordem(self) -> int:
        """0 = Snapshot, 1 = One Year, 2 = Three Year."""
        return VERSOES.index(self.versao)


def escopo() -> list[Fonte]:
    """O que o projeto carrega, em ordem de carga (D26, decidido com a medição da fase 0).

    - Série: a versão mais madura de cada ano, 2018-2025 (a "vigente").
    - Versões para as revisões: Snapshot e One Year de 2021 e 2022, que junto com o
      Three Year dão as três versões de dois anos completos.
    A ordem importa: dentro de um ano, da menos para a mais madura, para que cada
    troca de versão vire um commit da tabela vigente (time travel).
    """
    revisoes = {2021, 2022}
    fontes = []
    for ano in range(PRIMEIRO_ANO, 2026):
        maduras = [v for v in VERSOES if ano in DISPONIVEIS[v]]
        if ano in revisoes:
            fontes += [Fonte(ano, v) for v in maduras]
        else:
            fontes.append(Fonte(ano, maduras[-1]))
    return fontes


# Data em que o FFIEC "congelou" cada versão (o dado é como estava nesse dia).
# Lido de cfpb/hmda-frontend (src/data-publication/constants/*, freezeDate) em 06/10/2026.
CONGELAMENTO: dict[tuple[int, str], date] = {
    (2018, "snapshot"): date(2019, 8, 7),
    (2019, "snapshot"): date(2020, 4, 27),
    (2020, "snapshot"): date(2021, 5, 3),
    (2021, "snapshot"): date(2022, 4, 30),
    (2022, "snapshot"): date(2023, 5, 1),
    (2023, "snapshot"): date(2024, 5, 1),
    (2024, "snapshot"): date(2025, 5, 19),
    (2025, "snapshot"): date(2026, 6, 2),
    (2019, "one_year"): date(2022, 4, 5),
    (2020, "one_year"): date(2022, 4, 30),
    (2021, "one_year"): date(2023, 5, 1),
    (2022, "one_year"): date(2024, 5, 1),
    (2023, "one_year"): date(2025, 5, 19),
    (2024, "one_year"): date(2026, 6, 2),
    (2018, "three_year"): date(2021, 12, 31),
    (2019, "three_year"): date(2022, 12, 31),
    (2020, "three_year"): date(2023, 12, 31),
    (2021, "three_year"): date(2024, 12, 31),
    (2022, "three_year"): date(2025, 12, 31),
}


def congelado_em(fonte: Fonte) -> date:
    return CONGELAMENTO[(fonte.ano, fonte.versao)]


def ordem_de_publicacao(fontes: list[Fonte]) -> list[Fonte]:
    """Ordena como o FFIEC publicou: assim cada commit da silver vigente é um
    retrato do que existia naquela data, e o time travel responde "o que um
    analista via em maio de 2023?" (D28)."""
    return sorted(fontes, key=lambda f: (congelado_em(f), f.ano, f.ordem))
