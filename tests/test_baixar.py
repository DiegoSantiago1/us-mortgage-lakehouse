"""Testes do download contra um servidor HTTP local (sem internet).

O servidor imita o S3 do FFIEC: HEAD com Content-Length e ETag, GET com Range.
Os modos hostis simulam conexão que cai no meio e arquivo republicado.
"""

import hashlib
import http.server
import json
import ssl
import threading

import pytest

from hmda import baixar, manifesto
from hmda.fontes import Fonte


class Arquivo:
    def __init__(self, conteudo: bytes, etag: str):
        self.conteudo = conteudo
        self.etag = etag
        self.cortar_em: int | None = None  # simula queda de conexão
        self.gets = 0


@pytest.fixture
def servidor():
    arquivo = Arquivo(b"PK" + bytes(range(256)) * 4000, "etag-1")

    class Handler(http.server.BaseHTTPRequestHandler):
        def _cabecalhos(self, status, tamanho):
            self.send_response(status)
            self.send_header("Content-Length", str(tamanho))
            self.send_header("ETag", f'"{arquivo.etag}"')
            self.send_header("Last-Modified", "Wed, 15 Oct 2025 05:31:31 GMT")
            self.end_headers()

        def do_HEAD(self):
            self._cabecalhos(200, len(arquivo.conteudo))

        def do_GET(self):
            arquivo.gets += 1
            corpo = arquivo.conteudo
            status = 200
            faixa = self.headers.get("Range")
            if_range = (self.headers.get("If-Range") or "").strip('"')
            if faixa and if_range == arquivo.etag:
                inicio = int(faixa.removeprefix("bytes=").rstrip("-"))
                corpo, status = corpo[inicio:], 206
            if arquivo.cortar_em is not None:
                # Anuncia o tamanho todo, mas manda só uma parte (conexão caiu).
                self._cabecalhos(status, len(corpo))
                self.wfile.write(corpo[: arquivo.cortar_em])
                arquivo.cortar_em = None
                self.close_connection = True
                return
            self._cabecalhos(status, len(corpo))
            self.wfile.write(corpo)

        def log_message(self, *args):
            pass

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}/2021_public_lar_three_year_csv.zip"
    yield arquivo, url
    httpd.shutdown()


@pytest.fixture
def pastas(tmp_path):
    return tmp_path / "bruto", tmp_path / "manifesto.json"


FONTE = Fonte(2021, "three_year")
CTX = ssl.create_default_context()


def test_baixa_confere_hash_e_registra_no_manifesto(servidor, pastas):
    arquivo, url = servidor
    bruto, man = pastas
    reg = baixar.baixar(FONTE, bruto, man, CTX, url=url)

    esperado = hashlib.sha256(arquivo.conteudo).hexdigest()
    assert reg["sha256"] == esperado
    assert reg["arquivo"] == f"2021_public_lar_three_year_csv_{esperado[:12]}.zip"
    caminho = bruto / "2021" / "three_year" / reg["arquivo"]
    assert caminho.read_bytes() == arquivo.conteudo
    assert not list(caminho.parent.glob("*.part"))
    dados = json.loads(man.read_text(encoding="utf-8"))
    assert dados["2021/three_year"]["arquivos"][0]["etag"] == "etag-1"


def test_rodar_duas_vezes_nao_baixa_de_novo(servidor, pastas):
    arquivo, url = servidor
    bruto, man = pastas
    baixar.baixar(FONTE, bruto, man, CTX, url=url)
    baixar.baixar(FONTE, bruto, man, CTX, url=url)
    assert arquivo.gets == 1
    assert len(manifesto.carregar(man)["2021/three_year"]["arquivos"]) == 1


def test_conexao_que_cai_continua_de_onde_parou(servidor, pastas):
    arquivo, url = servidor
    bruto, man = pastas
    arquivo.cortar_em = 300_000
    with pytest.raises(baixar.ErroDownload, match="incompleto"):
        baixar.baixar(FONTE, bruto, man, CTX, url=url)
    pasta = bruto / "2021" / "three_year"
    assert not list(pasta.glob("*.zip"))  # nada com nome final
    assert manifesto.carregar(man) == {}  # nada registrado

    reg = baixar.baixar(FONTE, bruto, man, CTX, url=url)  # continua com Range
    assert reg["sha256"] == hashlib.sha256(arquivo.conteudo).hexdigest()
    assert (pasta / reg["arquivo"]).read_bytes() == arquivo.conteudo


def test_parte_de_arquivo_antigo_nao_contamina_o_novo(servidor, pastas):
    """Se o servidor trocou o arquivo entre a queda e a retomada, recomeça do zero."""
    arquivo, url = servidor
    bruto, man = pastas
    arquivo.cortar_em = 300_000
    with pytest.raises(baixar.ErroDownload):
        baixar.baixar(FONTE, bruto, man, CTX, url=url)
    arquivo.conteudo = b"PK" + b"novo" * 200_000
    arquivo.etag = "etag-2"
    reg = baixar.baixar(FONTE, bruto, man, CTX, url=url)
    assert reg["sha256"] == hashlib.sha256(arquivo.conteudo).hexdigest()
    assert not list((bruto / "2021" / "three_year").glob("*.part*"))


def test_mesmo_tamanho_mas_arquivo_trocado_nao_mistura(servidor, pastas):
    """Pior caso: o arquivo novo tem o MESMO tamanho; só o ETag denuncia a troca."""
    arquivo, url = servidor
    bruto, man = pastas
    arquivo.cortar_em = 300_000
    with pytest.raises(baixar.ErroDownload):
        baixar.baixar(FONTE, bruto, man, CTX, url=url)
    arquivo.conteudo = bytes(reversed(arquivo.conteudo))
    arquivo.etag = "etag-2"
    reg = baixar.baixar(FONTE, bruto, man, CTX, url=url)
    assert reg["sha256"] == hashlib.sha256(arquivo.conteudo).hexdigest()


def test_arquivo_republicado_vira_historico(servidor, pastas):
    arquivo, url = servidor
    bruto, man = pastas
    primeiro = baixar.baixar(FONTE, bruto, man, CTX, url=url)
    arquivo.conteudo = b"PK" + b"corrigido" * 100_000
    arquivo.etag = "etag-2"
    segundo = baixar.baixar(FONTE, bruto, man, CTX, url=url)

    assert primeiro["arquivo"] != segundo["arquivo"]
    pasta = bruto / "2021" / "three_year"
    assert (pasta / primeiro["arquivo"]).exists()  # o antigo não é apagado
    historico = manifesto.carregar(man)["2021/three_year"]["arquivos"]
    assert [a["etag"] for a in historico] == ["etag-1", "etag-2"]
    assert manifesto.arquivo_atual(manifesto.carregar(man), "2021/three_year") == segundo


def test_arquivo_apagado_do_disco_e_baixado_de_novo(servidor, pastas):
    arquivo, url = servidor
    bruto, man = pastas
    reg = baixar.baixar(FONTE, bruto, man, CTX, url=url)
    (bruto / "2021" / "three_year" / reg["arquivo"]).unlink()
    baixar.baixar(FONTE, bruto, man, CTX, url=url)
    assert arquivo.gets == 2


def test_manifesto_e_gravado_de_forma_atomica(tmp_path):
    man = tmp_path / "m.json"
    manifesto.salvar(man, {"a": 1})
    assert json.loads(man.read_text(encoding="utf-8")) == {"a": 1}
    assert not list(tmp_path.glob("*.tmp"))
    assert b"\r\n" not in man.read_bytes()
