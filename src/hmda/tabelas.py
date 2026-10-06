"""Nome lógico -> caminho de cada tabela Delta (D16: sem metastore).

Um lugar só para os caminhos. No Databricks (fase 7), os mesmos nomes viram
tabelas do catálogo.
"""

from pathlib import Path

BRONZE = "bronze/pedidos"
CARGAS = "controle/cargas"
SILVER = "silver/vigente"
REJEITADOS = "silver/rejeitados"
SILVER_VERSOES = "controle/silver_versoes"
REV_DIFERENCAS = "revisoes/diferencas"
REV_INSTITUICOES = "revisoes/instituicoes"
REV_CAMPOS = "revisoes/campos"
DQ = "dq/checagens"


def caminho(raiz: Path, nome: str) -> str:
    return str(raiz / nome)
