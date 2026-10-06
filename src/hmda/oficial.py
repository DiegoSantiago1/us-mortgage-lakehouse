"""Números oficiais do FFIEC para conferir a silver (D24).

    python -m hmda.oficial            # consulta a API e grava docs/oficial/contagens_<ano>.json

A API do Data Browser (`/v2/data-browser-api/view/aggregations`) usa a versão
mais recente de cada ano, que é a mesma que a silver vigente usa. Ela só
conta pedidos com estado: linhas com `state_code = NA` ficam de fora (medido
em 2021: 171.739 linhas).

O resultado vai para o repositório, com a data da consulta. Assim, a
conferência é reprodutível e roda no CI sem internet.
"""

import argparse
import json
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from hmda import config
from hmda.baixar import contexto_ssl

API = "https://ffiec.cfpb.gov/v2/data-browser-api/view/aggregations"
PASTA = Path(__file__).resolve().parents[2] / "docs" / "oficial"

# 50 estados + DC + territórios que aparecem no HMDA. FM, MH e PW (Estados Associados:
# Micronésia, Ilhas Marshall, Palau) faltavam na 1ª versão: o dq achou 1-2 linhas
# deles em 2018, 2020 e 2023 que a consulta não cobria (D38).
ESTADOS = tuple(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE "  # noqa: SIM905
    "NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY PR GU VI AS MP "
    "FM MH PW".split()
)
RESULTADOS = "1,2,3,4,5,6,7,8"


def consultar_estado(ano: int, estado: str, ctx: ssl.SSLContext) -> dict[str, dict[str, float]]:
    query = urllib.parse.urlencode(
        {"states": estado, "years": ano, "actions_taken": RESULTADOS}, safe=","
    )
    for tentativa in range(4):
        try:
            with urllib.request.urlopen(f"{API}?{query}", context=ctx, timeout=60) as r:
                corpo = json.load(r)
            break
        except OSError:
            if tentativa == 3:
                raise
            time.sleep(2**tentativa)
    return {
        a["actions_taken"]: {"contagem": a["count"], "soma_valor": a["sum"]}
        for a in corpo["aggregations"]
    }


def baixar_ano(ano: int, ctx: ssl.SSLContext) -> dict:
    por_estado = {}
    for estado in ESTADOS:
        por_estado[estado] = consultar_estado(ano, estado, ctx)
        time.sleep(0.2)  # educação com a API pública
    return {
        "fonte": API,
        "consultado_em": datetime.now(UTC).isoformat(timespec="seconds"),
        "ano": ano,
        "observacao": "versão mais recente do ano; só pedidos com estado",
        "por_estado": por_estado,
    }


def carregar(ano: int, pasta: Path = PASTA) -> dict:
    return json.loads((pasta / f"contagens_{ano}.json").read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--anos", type=int, nargs="+", default=list(range(2018, 2026)))
    args = p.parse_args(argv)
    ctx = contexto_ssl(config.carregar().ca_extra)
    PASTA.mkdir(parents=True, exist_ok=True)
    for ano in args.anos:
        dados = baixar_ano(ano, ctx)
        total = sum(v["contagem"] for e in dados["por_estado"].values() for v in e.values())
        (PASTA / f"contagens_{ano}.json").write_text(
            json.dumps(dados, indent=1, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
        )
        print(f"{ano}: {total:,} pedidos com estado")
    return 0


if __name__ == "__main__":
    sys.exit(main())
