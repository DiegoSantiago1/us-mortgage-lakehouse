"""Testes do vigia de publicações (D44): quando o pipeline precisa rodar e quando não."""

import http.server
import ssl
import threading
import urllib.error

import pytest

from hmda import vigia
from hmda.baixar import Remoto, consultar
from hmda.fontes import Fonte, escopo, url_de

F21 = Fonte(2021, "three_year")
F25 = Fonte(2025, "snapshot")


def _manifesto(*itens: tuple[Fonte, str, int]) -> dict:
    return {
        f.chave: {"arquivos": [{"arquivo": "x.zip", "etag": etag, "bytes": tamanho}]}
        for f, etag, tamanho in itens
    }


def _servidor(respostas: dict[str, object]):
    """Consulta falsa: URL → Remoto, ou uma exceção para levantar."""

    def consultar_fn(url: str) -> Remoto:
        resposta = respostas.get(url, urllib.error.HTTPError(url, 404, "Not Found", {}, None))
        if isinstance(resposta, Exception):
            raise resposta
        assert isinstance(resposta, Remoto)
        return resposta

    return consultar_fn


def test_nada_mudou_nao_precisa_rodar() -> None:
    dados = _manifesto((F21, "e1", 100), (F25, "e2", 200))
    fn = _servidor({F21.url: Remoto(100, "e1", ""), F25.url: Remoto(200, "e2", "")})
    rel = vigia.verificar(dados, fn, fontes=[F21, F25], proximas=[(2026, "snapshot")])
    assert [s.estado for s in rel.escopo] == [vigia.IGUAL, vigia.IGUAL]
    assert not rel.precisa_rodar
    assert rel.novas == [] and rel.erros == []
    assert "nada mudou" in rel.resumo()


def test_arquivo_republicado_precisa_rodar() -> None:
    dados = _manifesto((F21, "e1", 100))
    fn = _servidor({F21.url: Remoto(105, "e9", "")})
    rel = vigia.verificar(dados, fn, fontes=[F21], proximas=[])
    assert rel.escopo[0].estado == vigia.REPUBLICADO
    assert "e1" in rel.escopo[0].detalhe and "e9" in rel.escopo[0].detalhe
    assert rel.precisa_rodar
    assert "rodar o pipeline" in rel.resumo()


def test_mesmo_etag_tamanho_diferente_tambem_conta_como_republicado() -> None:
    dados = _manifesto((F21, "e1", 100))
    rel = vigia.verificar(dados, _servidor({F21.url: Remoto(99, "e1", "")}), [F21], [])
    assert rel.escopo[0].estado == vigia.REPUBLICADO


def test_arquivo_nunca_baixado_precisa_rodar() -> None:
    rel = vigia.verificar({}, _servidor({F21.url: Remoto(1, "e", "")}), [F21], [])
    assert rel.escopo[0].estado == vigia.NAO_BAIXADO
    assert rel.precisa_rodar


def test_falha_de_rede_vira_erro_e_nao_decide_rodar() -> None:
    dados = _manifesto((F21, "e1", 100))
    fn = _servidor({F21.url: urllib.error.URLError("timed out")})
    rel = vigia.verificar(dados, fn, [F21], [])
    assert rel.escopo[0].estado == vigia.ERRO
    assert rel.erros and "timed out" in rel.erros[0]
    assert not rel.precisa_rodar  # quem decide com erro é o DAG: ele falha a tarefa


def test_versao_nova_publicada_fora_do_projeto_vira_aviso() -> None:
    publicada = url_de(2026, "snapshot")
    fn = _servidor({publicada: Remoto(1, "x", "")})
    rel = vigia.verificar({}, fn, fontes=[], proximas=[(2026, "snapshot"), (2025, "one_year")])
    assert rel.novas == ["2026/snapshot"]
    assert not rel.precisa_rodar  # ano novo pede decisão humana (fontes.DISPONIVEIS)
    assert "2026/snapshot" in rel.resumo()


def test_erro_do_servidor_na_versao_nova_nao_e_confundido_com_nao_publicada() -> None:
    url = url_de(2026, "snapshot")
    fn = _servidor({url: urllib.error.HTTPError(url, 500, "Internal Server Error", {}, None)})
    rel = vigia.verificar({}, fn, fontes=[], proximas=[(2026, "snapshot")])
    assert rel.novas == []
    assert rel.erros and "500" in rel.erros[0]


def test_candidatas_sao_o_proximo_ano_de_cada_versao() -> None:
    assert vigia.candidatas() == [(2026, "snapshot"), (2025, "one_year"), (2023, "three_year")]


def test_url_de_e_a_mesma_regra_do_escopo() -> None:
    for fonte in escopo():
        assert url_de(fonte.ano, fonte.versao) == fonte.url


@pytest.fixture
def servidor_404():
    """Servidor HTTP de verdade: só o arquivo de 2021 existe; o resto dá 404."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_HEAD(self):
            if self.path.endswith("2021_public_lar_three_year_csv.zip"):
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.send_header("ETag", '"e1"')
            else:
                self.send_response(404)
            self.end_headers()

        def log_message(self, *args):
            pass

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_com_http_de_verdade_404_e_nao_publicada(servidor_404, monkeypatch) -> None:
    """A consulta real (urllib) levanta HTTPError no 404; o vigia trata como 'não saiu'."""
    monkeypatch.setattr("hmda.fontes.BASE", servidor_404)
    ctx = ssl.create_default_context()
    rel = vigia.verificar(
        _manifesto((F21, "e1", 100)),
        lambda url: consultar(url, ctx),
        fontes=[F21],
        proximas=[(2026, "snapshot")],
    )
    assert rel.escopo[0].estado == vigia.IGUAL
    assert rel.novas == [] and rel.erros == []


def test_decidir() -> None:
    igual = vigia.Relatorio(escopo=[vigia.Situacao("2021/three_year", vigia.IGUAL)])
    mudou = vigia.Relatorio(escopo=[vigia.Situacao("2021/three_year", vigia.REPUBLICADO)])
    assert vigia.decidir(igual) is False
    assert vigia.decidir(igual, forcar=True) is True
    assert vigia.decidir(mudou) is True


def test_decidir_com_erro_falha_mesmo_forcando() -> None:
    rel = vigia.Relatorio(erros=["2021/three_year: timed out"])
    with pytest.raises(vigia.ErroVigia, match="timed out"):
        vigia.decidir(rel, forcar=True)
