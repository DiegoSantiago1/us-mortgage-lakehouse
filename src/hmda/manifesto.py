"""Manifesto dos arquivos baixados: o que foi baixado, de onde, quando e com que hash.

É um JSON legível (abre no VS Code) gravado de forma atômica: escreve num
temporário e troca pelo nome final, então um processo interrompido nunca deixa
o manifesto pela metade.
"""

import json
import os
from pathlib import Path
from typing import Any


def carregar(caminho: Path) -> dict[str, Any]:
    if not caminho.exists():
        return {}
    return json.loads(caminho.read_text(encoding="utf-8"))


def salvar(caminho: Path, dados: dict[str, Any]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    temporario = caminho.with_suffix(caminho.suffix + ".tmp")
    with temporario.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporario, caminho)


def arquivo_atual(manifesto: dict[str, Any], chave: str) -> dict[str, Any] | None:
    """O último arquivo baixado para ano/versão (o FFIEC às vezes republica)."""
    entrada = manifesto.get(chave)
    if not entrada or not entrada.get("arquivos"):
        return None
    return entrada["arquivos"][-1]
