"""Baixa o arquivo nacional do HMDA de um ano e versão, de forma idempotente.

    python -m hmda.baixar --ano 2021 --versao three_year
    python -m hmda.baixar --escopo          # tudo o que o projeto usa

- Pergunta ao servidor (HEAD) o tamanho e o ETag. Se o manifesto já tem esse
  mesmo arquivo e ele está no disco com o tamanho certo, não baixa de novo.
- Baixa para um `.part`, calculando o SHA-256 no caminho; se a conexão cair,
  a próxima execução continua de onde parou (Range).
- Só depois de conferir o tamanho o arquivo ganha o nome final, com o começo do
  hash no nome. Se o FFIEC republicar o arquivo (ETag novo), o antigo é mantido
  e o manifesto guarda o histórico.
"""

import argparse
import hashlib
import ssl
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hmda import config, manifesto
from hmda.fontes import VERSOES, Fonte, escopo

BLOCO = 1024 * 1024  # 1 MiB


class ErroDownload(RuntimeError):
    pass


@dataclass(frozen=True)
class Remoto:
    tamanho: int
    etag: str
    modificado_em: str


def contexto_ssl(ca_extra: Path | None) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if ca_extra:
        # Soma (não substitui) o certificado do antivírus aos do sistema.
        ctx.load_verify_locations(cafile=str(ca_extra))
    return ctx


def consultar(url: str, ctx: ssl.SSLContext) -> Remoto:
    pedido = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(pedido, context=ctx, timeout=60) as r:
        tamanho = r.headers.get("Content-Length")
        if tamanho is None:
            raise ErroDownload(f"servidor não informou o tamanho de {url}")
        return Remoto(
            tamanho=int(tamanho),
            etag=(r.headers.get("ETag") or "").strip('"'),
            modificado_em=r.headers.get("Last-Modified") or "",
        )


def _hash_parcial(parte: Path) -> "hashlib._Hash":
    h = hashlib.sha256()
    with parte.open("rb") as f:
        while bloco := f.read(BLOCO):
            h.update(bloco)
    return h


def transferir(url: str, destino_parte: Path, remoto: Remoto, ctx: ssl.SSLContext) -> str:
    """Baixa (ou continua baixando) para `destino_parte`; devolve o SHA-256.

    O ETag do pedaço já baixado fica ao lado (`.part.etag`). Se o servidor trocou
    o arquivo depois da queda, o pedaço antigo é descartado: juntar o começo de um
    arquivo com o fim de outro daria um zip corrompido, às vezes com o tamanho certo.
    """
    sidecar = destino_parte.with_name(destino_parte.name + ".etag")
    etag_da_parte = sidecar.read_text(encoding="utf-8") if sidecar.exists() else None
    ja_tem = destino_parte.stat().st_size if destino_parte.exists() else 0
    if ja_tem and (etag_da_parte != remoto.etag or ja_tem > remoto.tamanho):
        destino_parte.unlink()
        ja_tem = 0
    sidecar.write_text(remoto.etag, encoding="utf-8")
    h = _hash_parcial(destino_parte) if ja_tem else hashlib.sha256()

    pedido = urllib.request.Request(url)
    if ja_tem:
        pedido.add_header("Range", f"bytes={ja_tem}-")
        # Se o arquivo mudou no servidor, o Range não vale: o servidor manda tudo (200).
        pedido.add_header("If-Range", f'"{remoto.etag}"')

    with urllib.request.urlopen(pedido, context=ctx, timeout=120) as r:
        if ja_tem and r.status != 206:
            ja_tem = 0
            h = hashlib.sha256()
        modo = "ab" if ja_tem else "wb"
        recebidos = ja_tem
        proximo_aviso = 0.0
        inicio = time.monotonic()
        with destino_parte.open(modo) as f:
            while bloco := r.read(BLOCO):
                f.write(bloco)
                h.update(bloco)
                recebidos += len(bloco)
                fracao = recebidos / remoto.tamanho
                if fracao >= proximo_aviso:
                    mb_s = (recebidos - ja_tem) / BLOCO / max(time.monotonic() - inicio, 1e-6)
                    print(f"  {fracao:6.1%}  {recebidos / BLOCO:,.0f} MiB  ({mb_s:.1f} MiB/s)")
                    proximo_aviso += 0.1

    if recebidos != remoto.tamanho:
        raise ErroDownload(
            f"download incompleto: {recebidos} de {remoto.tamanho} bytes "
            f"(rode de novo para continuar)"
        )
    sidecar.unlink()
    return h.hexdigest()


def baixar(
    fonte: Fonte,
    pasta_bruto: Path,
    caminho_manifesto: Path,
    ctx: ssl.SSLContext,
    url: str | None = None,
) -> dict[str, Any]:
    """Devolve o registro do arquivo atual de `fonte` no manifesto."""
    url = url or fonte.url
    remoto = consultar(url, ctx)
    dados = manifesto.carregar(caminho_manifesto)
    atual = manifesto.arquivo_atual(dados, fonte.chave)
    pasta = pasta_bruto / str(fonte.ano) / fonte.versao

    if (
        atual
        and atual["etag"] == remoto.etag
        and atual["bytes"] == remoto.tamanho
        and (pasta / atual["arquivo"]).is_file()
        and (pasta / atual["arquivo"]).stat().st_size == remoto.tamanho
    ):
        print(f"{fonte.chave}: já baixado ({atual['arquivo']}), nada a fazer")
        return atual

    pasta.mkdir(parents=True, exist_ok=True)
    nome_base = url.rsplit("/", 1)[-1].removesuffix(".zip")
    parte = pasta / f"{nome_base}.zip.part"
    print(f"{fonte.chave}: baixando {remoto.tamanho / BLOCO:,.0f} MiB de {url}")
    sha = transferir(url, parte, remoto, ctx)

    final = pasta / f"{nome_base}_{sha[:12]}.zip"
    parte.replace(final)
    registro = {
        "arquivo": final.name,
        "url": url,
        "bytes": remoto.tamanho,
        "etag": remoto.etag,
        "modificado_no_servidor": remoto.modificado_em,
        "sha256": sha,
        "baixado_em": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    entrada = dados.setdefault(fonte.chave, {"ano": fonte.ano, "versao": fonte.versao})
    entrada.setdefault("arquivos", []).append(registro)
    manifesto.salvar(caminho_manifesto, dados)
    print(f"{fonte.chave}: ok, sha256 {sha[:12]}…")
    return registro


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--ano", type=int)
    p.add_argument("--versao", choices=VERSOES)
    p.add_argument("--escopo", action="store_true", help="baixa tudo o que o projeto usa")
    args = p.parse_args(argv)
    if args.escopo == (args.ano is not None or args.versao is not None):
        p.error("use --escopo OU --ano e --versao")

    cfg = config.carregar()
    ctx = contexto_ssl(cfg.ca_extra)
    try:
        fontes = escopo() if args.escopo else [Fonte(args.ano, args.versao)]
        for fonte in fontes:
            baixar(fonte, cfg.bruto, cfg.manifesto, ctx)
    except (ValueError, ErroDownload) as erro:
        print(f"erro: {erro}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
