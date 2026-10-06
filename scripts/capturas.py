"""Confere a página de resultados num navegador de verdade (Playwright, headless).

    <python com playwright> scripts/capturas.py <pasta_saida>

Sobe um servidor local para site/, abre em desktop e celular, claro e escuro,
português e inglês; tira captura do viewport em cada capítulo e falha se houver
erro no console ou se algum gráfico ficar vazio.
"""

import functools
import http.server
import shutil
import sys
import tempfile
import threading
from pathlib import Path

from playwright.sync_api import sync_playwright

SITE = Path(__file__).resolve().parents[1] / "site"
CAPITULOS = ["inicio", "c1", "c2", "c3", "c4", "c5", "c6", "c7"]
GRAFICOS = [
    "linha-tempo", "mapa", "linha-preco", "barras-msa", "linha-volume", "linha-juros",
    "linha-negativa", "barras-motivos", "barras-equidade", "barras-bancos", "barras-campos",
    "tabela-impacto", "arquitetura",
]


def servir() -> tuple[http.server.ThreadingHTTPServer, str]:
    # Servir direto da pasta do OneDrive dá ERR_CONNECTION_RESET em arquivos maiores
    # (lição do Projeto 3): copia para uma pasta temporária fora dele.
    copia = Path(tempfile.mkdtemp()) / "site"
    shutil.copytree(SITE, copia)
    class Quieto(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

    handler = functools.partial(Quieto, directory=str(copia))
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}/"


def main(saida: Path) -> int:
    saida.mkdir(parents=True, exist_ok=True)
    httpd, url = servir()
    problemas: list[str] = []
    cenarios = [
        ("desktop-claro-pt", {"width": 1366, "height": 900}, "light", "pt"),
        ("desktop-escuro-en", {"width": 1366, "height": 900}, "dark", "en"),
        ("celular-claro-pt", {"width": 390, "height": 844}, "light", "pt"),
        ("celular-escuro-pt", {"width": 390, "height": 844}, "dark", "pt"),
    ]
    with sync_playwright() as p:
        navegador = p.chromium.launch()
        for nome, viewport, tema, idioma in cenarios:
            ctx = navegador.new_context(
                viewport=viewport, color_scheme=tema, locale=idioma, reduced_motion="reduce"
            )
            pagina = ctx.new_page()
            erros: list[str] = []
            pagina.on("console", lambda m, e=erros: e.append(m.text) if m.type == "error" else None)
            pagina.on("pageerror", lambda ex, e=erros: e.append(str(ex)))
            pagina.goto(url, wait_until="load")
            # O servidor local do Windows às vezes reseta a conexão: uma nova tentativa.
            if pagina.evaluate("document.querySelectorAll('main > p.aviso').length"):
                erros.clear()
                pagina.reload(wait_until="load")
            try:
                pagina.wait_for_function(
                    "document.querySelector('#mapa svg path.estado') !== null", timeout=60000
                )
            except Exception:  # noqa: BLE001
                aviso = pagina.evaluate("[...document.querySelectorAll('main > p.aviso')].map(e => e.textContent)")
                problemas.append(f"{nome}: o mapa não desenhou; aviso={aviso}; erros={erros}")
                ctx.close()
                continue
            pagina.wait_for_timeout(800)
            for g in GRAFICOS:
                vazio = pagina.evaluate(f"!document.getElementById('{g}') || !document.getElementById('{g}').children.length")
                if vazio:
                    problemas.append(f"{nome}: gráfico vazio #{g}")
            largura = pagina.evaluate("document.documentElement.scrollWidth")
            if largura > viewport["width"] + 1:
                problemas.append(f"{nome}: rolagem horizontal ({largura}px > {viewport['width']}px)")
            for cap in CAPITULOS:
                pagina.evaluate(f"document.getElementById('{cap}').scrollIntoView()")
                pagina.wait_for_timeout(250)
                pagina.screenshot(path=str(saida / f"{nome}-{cap}.png"))
            problemas += [f"{nome}: console: {e}" for e in erros]
            ctx.close()
        navegador.close()
    httpd.shutdown()
    for p_ in problemas:
        print("PROBLEMA", p_)
    print(f"{len(problemas)} problema(s); capturas em {saida}")
    return 1 if problemas else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
