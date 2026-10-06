"""Caminhos e parâmetros lidos do ambiente.

Dentro do container:
- /dados  = pasta do PC (HMDA_DADOS_HOST): bruto (zips) e manifesto. É o que precisa durar.
- /lake   = volume do Docker: tabelas Delta e CSV temporário. É derivado (dá para
            reconstruir a partir dos zips) e é 2x mais rápido de ler (medido, D25).
Nos testes, as duas apontam para pastas temporárias.
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    dados: Path
    lake: Path
    ca_extra: Path | None
    driver_memory: str

    @property
    def bruto(self) -> Path:
        """Zips baixados do FFIEC, intocados e com o hash no nome (D22)."""
        return self.dados / "bruto"

    @property
    def manifesto(self) -> Path:
        return self.dados / "manifesto.json"

    @property
    def tabelas(self) -> Path:
        """Tabelas Delta (bronze, silver, gold...)."""
        return self.lake / "tabelas"

    @property
    def temporario(self) -> Path:
        """CSV extraído do zip enquanto a bronze carrega (apagado em seguida)."""
        return self.lake / "tmp"


def carregar() -> Config:
    ca = os.environ.get("HMDA_CA_EXTRA", "").strip()
    return Config(
        dados=Path(os.environ.get("HMDA_DADOS", "/dados")),
        lake=Path(os.environ.get("HMDA_LAKE", "/lake")),
        ca_extra=Path(ca) if ca else None,
        driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "6g"),
    )
