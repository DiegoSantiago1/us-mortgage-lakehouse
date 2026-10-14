"""Experimento: o Z-ORDER por estado faz o filtro `estado = 'TX'` pular arquivos? (D43)

Na silver, todo arquivo tem pedidos de todos os estados, então o mínimo e o máximo que o
Delta guarda por arquivo não deixam pular nada (docs/CATALOGO.md: 109 de 109 arquivos).
Aqui uma cópia de um ano é reorganizada com `OPTIMIZE ... ZORDER BY (estado)` e a mesma
consulta é medida antes e depois: arquivos lidos e tempo.

A silver de verdade não é tocada: a cópia fica em /lake/experimentos (fora de
/lake/tabelas, para não entrar no catálogo).

Uso:
    python -m hmda.zorder --relatorio docs/ZORDER.md
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from hmda import tabelas
from hmda.config import carregar
from hmda.spark import criar_sessao


@dataclass(frozen=True)
class Medicao:
    arquivos_total: int
    arquivos_lidos: int
    segundos: float
    linhas: int


def medir(spark: SparkSession, caminho: str, estado: str) -> Medicao:
    """Arquivos lidos e tempo de uma contagem filtrada por estado (melhor de 3 execuções)."""
    tabela = spark.read.format("delta").load(caminho)
    filtrada = tabela.where(tabela.estado == estado)
    tempos = []
    linhas = 0
    for _ in range(3):
        inicio = time.perf_counter()
        linhas = filtrada.count()
        tempos.append(time.perf_counter() - inicio)
    return Medicao(
        arquivos_total=len(tabela.inputFiles()),
        arquivos_lidos=len(filtrada.inputFiles()),
        segundos=min(tempos),
        linhas=linhas,
    )


def preparar_copia(spark: SparkSession, origem: str, destino: Path, ano: int, arquivos: int) -> str:
    """Copia um ano da silver numa tabela Delta nova, com estatísticas de `estado`.

    `arquivos` fixa quantos arquivos a cópia tem antes do Z-ORDER, para a comparação ser
    justa (mesmo número de arquivos antes e depois).
    """
    if destino.exists():
        shutil.rmtree(destino)
    caminho = str(destino)
    (
        spark.read.format("delta")
        .load(origem)
        .where(f"ano = {int(ano)}")
        .repartition(arquivos)  # embaralha: cada arquivo fica com todos os estados
        .write.format("delta")
        # Garante min/max de `estado` em cada arquivo (por padrão, só as 32 primeiras colunas).
        .option("delta.dataSkippingStatsColumns", "estado")
        .save(caminho)
    )
    return caminho


def zorder(spark: SparkSession, caminho: str, bytes_por_arquivo: int | None = None) -> None:
    """Reorganiza por estado. Sem limite, o OPTIMIZE junta tudo em arquivos de até 1 GB
    (medido: os 14 arquivos de 2022 viraram 1, e com um arquivo só não há o que pular).
    Com `bytes_por_arquivo`, o número de arquivos fica parecido com o de antes."""
    chave = "spark.databricks.delta.optimize.maxFileSize"
    if bytes_por_arquivo is not None:
        spark.conf.set(chave, str(bytes_por_arquivo))
    try:
        spark.sql(f"OPTIMIZE delta.`{caminho}` ZORDER BY (estado)")
    finally:
        if bytes_por_arquivo is not None:
            spark.conf.unset(chave)


def experimento(
    spark: SparkSession, raiz: Path, destino: Path, ano: int = 2022, estado: str = "TX"
) -> tuple[Medicao, Medicao]:
    origem = tabelas.caminho(raiz, tabelas.SILVER)
    # Mesmo número de arquivos da partição original do ano (docs/CATALOGO.md: 14 em 2022).
    arquivos_ano = len(
        spark.read.format("delta").load(origem).where(f"ano = {int(ano)}").inputFiles()
    )
    arquivos = max(arquivos_ano, 2)
    caminho = preparar_copia(spark, origem, destino, ano, arquivos)
    antes = medir(spark, caminho, estado)
    detalhe = spark.sql(f"DESCRIBE DETAIL delta.`{caminho}`").first()
    tamanho = int(detalhe["sizeInBytes"]) if detalhe is not None else 0
    zorder(spark, caminho, max(tamanho // arquivos, 1))
    depois = medir(spark, caminho, estado)
    return antes, depois


def conclusao(antes: Medicao, depois: Medicao) -> str:
    """O texto sai dos números: só afirma o que a medição mostrou."""
    if antes.linhas != depois.linhas:
        return "**Atenção:** o número de linhas mudou; a medição não vale."
    if depois.arquivos_total < 2:
        return "O OPTIMIZE deixou um arquivo só: não há o que pular, e a comparação não vale."
    if depois.arquivos_lidos < antes.arquivos_lidos:
        return (
            "O número de linhas é o mesmo: o Z-ORDER só reorganiza. Depois dele, cada arquivo "
            "cobre uma faixa de estados, e o mínimo e o máximo de `estado` que o Delta guarda "
            "por arquivo deixam pular os que não podem ter o estado pedido (data skipping). "
            "Antes, todo arquivo tinha todos os estados e nada podia ser pulado."
        )
    return "O Z-ORDER não reduziu os arquivos lidos nesta medição."


def relatorio(antes: Medicao, depois: Medicao, ano: int, estado: str, destino: Path) -> str:
    def pct(m: Medicao) -> str:
        return f"{100 * m.arquivos_lidos / m.arquivos_total:.0f}%" if m.arquivos_total else "-"

    def linha(rotulo: str, m: Medicao) -> str:
        return (
            f"| {rotulo} | {m.arquivos_total} | {m.arquivos_lidos} ({pct(m)}) | "
            f"{m.segundos:.2f} s | {m.linhas:,} |"
        )

    texto = "\n".join(
        [
            "# Z-ORDER por estado: medição (gerado por `python -m hmda.zorder`)",
            "",
            f"Cópia da silver de {ano} (a silver de verdade não é tocada), consulta "
            f"`SELECT COUNT(*) WHERE estado = '{estado}'`, melhor tempo de 3 execuções.",
            "",
            "| | Arquivos | Arquivos lidos | Tempo | Linhas |",
            "|---|---|---|---|---|",
            linha("Antes (cada arquivo com todos os estados)", antes),
            linha("Depois de `OPTIMIZE ... ZORDER BY (estado)`", depois),
            "",
            conclusao(antes, depois),
            "",
            "**Por que não foi aplicado na silver:** as análises do projeto agregam o país inteiro "
            "(todos os estados), então o ganho não aparece nelas, e o Z-ORDER reescreve todos os "
            "arquivos a cada carga. Vale para uma tabela consultada estado a estado.",
            "",
        ]
    )
    destino.write_text(texto, encoding="utf-8", newline="\n")
    return texto


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Mede o efeito do Z-ORDER por estado.")
    parser.add_argument("--ano", type=int, default=2022)
    parser.add_argument("--estado", default="TX")
    parser.add_argument("--relatorio", type=Path, help="grava o resultado neste arquivo")
    args = parser.parse_args(argv)
    config = carregar()
    spark = criar_sessao("hmda-zorder", driver_memory=config.driver_memory)
    try:
        destino = config.lake / "experimentos" / f"zorder_{args.ano}"
        antes, depois = experimento(spark, config.tabelas, destino, args.ano, args.estado)
    finally:
        spark.stop()
    print(f"antes:  {antes}")
    print(f"depois: {depois}")
    if args.relatorio:
        relatorio(antes, depois, args.ano, args.estado, args.relatorio)
        print(f"relatório: {args.relatorio}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
