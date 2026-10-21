"""Vigia de publicações do FFIEC: decide se o pipeline precisa rodar (D44).

    python -m hmda.vigia            # mostra a situação; sai com 1 se não conseguiu conferir

O governo publica e republica os arquivos sem aviso. Duas perguntas, só com HEAD (nada é
baixado):

1. **Algum arquivo que o projeto usa mudou?** O ETag no servidor é comparado com o do
   manifesto. ETag diferente (republicação) ou arquivo nunca baixado = o pipeline precisa
   rodar.
2. **Saiu alguma versão que o projeto ainda não usa?** Pergunta pelo próximo ano de cada
   versão (ex.: Snapshot 2026, One Year 2025). Se responder 200, é um aviso: entrar com um
   ano novo pede mudar `fontes.DISPONIVEIS` e rever o escopo, decisão que não deve ser
   automática.

É usado pelo DAG do Airflow (airflow/dags/hmda_lakehouse.py): se nada mudou, as tarefas
seguintes nem começam.
"""

from __future__ import annotations

import sys
import urllib.error
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from hmda import config, manifesto
from hmda.baixar import Remoto, consultar, contexto_ssl
from hmda.fontes import DISPONIVEIS, VERSOES, Fonte, escopo, url_de

IGUAL = "igual"
REPUBLICADO = "republicado"
NAO_BAIXADO = "nao_baixado"
ERRO = "erro"

Consultor = Callable[[str], Remoto]


@dataclass(frozen=True)
class Situacao:
    chave: str
    estado: str
    detalhe: str = ""


@dataclass
class Relatorio:
    escopo: list[Situacao] = field(default_factory=list)
    novas: list[str] = field(default_factory=list)  # publicadas e fora do projeto
    erros: list[str] = field(default_factory=list)

    @property
    def mudados(self) -> list[Situacao]:
        return [s for s in self.escopo if s.estado in (REPUBLICADO, NAO_BAIXADO)]

    @property
    def precisa_rodar(self) -> bool:
        return bool(self.mudados)

    def resumo(self) -> str:
        linhas = [
            f"{s.chave}: {s.estado}" + (f" ({s.detalhe})" if s.detalhe else "") for s in self.escopo
        ]
        if self.novas:
            linhas.append("versões publicadas fora do projeto: " + ", ".join(self.novas))
        else:
            linhas.append("nenhuma versão nova publicada fora do projeto")
        linhas += [f"erro: {e}" for e in self.erros]
        linhas.append(
            "decisão: rodar o pipeline"
            if self.precisa_rodar
            else "decisão: nada mudou, o pipeline não precisa rodar"
        )
        return "\n".join(linhas)


def candidatas() -> list[tuple[int, str]]:
    """O próximo ano de cada versão, ainda não publicado quando o projeto foi feito."""
    return [(max(DISPONIVEIS[v]) + 1, v) for v in VERSOES]


def _situacao(fonte: Fonte, dados: dict[str, Any], consultar_fn: Consultor) -> Situacao:
    atual = manifesto.arquivo_atual(dados, fonte.chave)
    try:
        remoto = consultar_fn(fonte.url)
    except (urllib.error.URLError, OSError, ValueError) as erro:
        return Situacao(fonte.chave, ERRO, str(erro))
    if atual is None:
        return Situacao(fonte.chave, NAO_BAIXADO)
    if atual["etag"] != remoto.etag or atual["bytes"] != remoto.tamanho:
        return Situacao(
            fonte.chave,
            REPUBLICADO,
            f"ETag {atual['etag'][:12]} → {remoto.etag[:12]}, "
            f"{atual['bytes']} → {remoto.tamanho} bytes",
        )
    return Situacao(fonte.chave, IGUAL)


def _publicada(url: str, consultar_fn: Consultor) -> bool:
    try:
        consultar_fn(url)
    except urllib.error.HTTPError as erro:
        # O bucket do FFIEC responde 403 ou 404 para o que não existe.
        if erro.code in (403, 404):
            return False
        raise
    return True


def verificar(
    dados: dict[str, Any],
    consultar_fn: Consultor,
    fontes: Iterable[Fonte] | None = None,
    proximas: Iterable[tuple[int, str]] | None = None,
) -> Relatorio:
    rel = Relatorio()
    for fonte in escopo() if fontes is None else fontes:
        situacao = _situacao(fonte, dados, consultar_fn)
        rel.escopo.append(situacao)
        if situacao.estado == ERRO:
            rel.erros.append(f"{situacao.chave}: {situacao.detalhe}")
    for ano, versao in candidatas() if proximas is None else proximas:
        try:
            if _publicada(url_de(ano, versao), consultar_fn):
                rel.novas.append(f"{ano}/{versao}")
        except (urllib.error.URLError, OSError, ValueError) as erro:
            rel.erros.append(f"{ano}/{versao}: {erro}")
    return rel


class ErroVigia(RuntimeError):
    pass


def decidir(rel: Relatorio, forcar: bool = False) -> bool:
    """O pipeline roda se algo mudou (ou se pediram para forçar). Se não deu para conferir,
    não dá para afirmar que nada mudou: levanta erro em vez de pular em silêncio."""
    if rel.erros:
        raise ErroVigia("não consegui conferir o FFIEC: " + "; ".join(rel.erros))
    return rel.precisa_rodar or forcar


def conferir_agora() -> Relatorio:
    """A conferência de verdade: manifesto do disco e HEAD no servidor do FFIEC."""
    cfg = config.carregar()
    ctx = contexto_ssl(cfg.ca_extra)
    return verificar(manifesto.carregar(cfg.manifesto), lambda url: consultar(url, ctx))


def main() -> int:
    rel = conferir_agora()
    print(rel.resumo())
    return 1 if rel.erros else 0


if __name__ == "__main__":
    sys.exit(main())
