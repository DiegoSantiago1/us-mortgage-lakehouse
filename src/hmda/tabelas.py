"""Nome lógico -> caminho de cada tabela Delta (D16: sem metastore).

Um lugar só para os caminhos. No Databricks (fase 7), os mesmos nomes viram
tabelas do catálogo.
"""

from pathlib import Path

BRONZE = "bronze/pedidos"
CARGAS = "controle/cargas"


def caminho(raiz: Path, nome: str) -> str:
    return str(raiz / nome)
