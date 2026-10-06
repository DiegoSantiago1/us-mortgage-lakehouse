"""Fase 0: mede um ano do HMDA de ponta a ponta para decidir o escopo.

    python scripts/medir_fase0.py /dados/bruto/2021/three_year/<arquivo>.zip

Mede: conteúdo do zip, tempo para extrair, formato do CSV, linhas, tempo de
CSV -> Delta (tudo texto) em dois destinos (pasta do Windows montada × volume
do Docker, risco R1), tamanho do Delta, tempo de leitura e pico de memória do
container. Grava o resultado em /dados/medicoes/.
"""

import json
import shutil
import sys
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from hmda.spark import criar_sessao


def tamanho_pasta(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def pico_memoria_container() -> int | None:
    for arq in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
        try:
            return int(Path(arq).read_text().strip())
        except (OSError, ValueError):
            continue
    return None


def cronometro():
    inicio = time.monotonic()
    return lambda: round(time.monotonic() - inicio, 1)


def main(zip_path: str) -> None:
    zp = Path(zip_path)
    r: dict = {
        "zip": zp.name,
        "zip_bytes": zp.stat().st_size,
        "medido_em": datetime.now(UTC).isoformat(),
    }

    with zipfile.ZipFile(zp) as z:
        r["membros"] = [{"nome": i.filename, "bytes": i.file_size} for i in z.infolist()]
        # O zip do FFIEC traz lixo do macOS (__MACOSX/._arquivo); só o CSV interessa.
        csvs = [
            i
            for i in z.infolist()
            if i.filename.endswith(".csv") and not i.filename.startswith("__MACOSX/")
        ]
        assert len(csvs) == 1, f"esperava 1 arquivo no zip, achei {len(csvs)}"
        membro = csvs[0]
        destinos = {"bind_windows": Path("/dados/medicao"), "volume_docker": Path("/lake/medicao")}
        for d in destinos.values():
            shutil.rmtree(d, ignore_errors=True)
            d.mkdir(parents=True)
        t = cronometro()
        csv_path = Path(z.extract(membro, destinos["volume_docker"]))
        r["extrair_no_volume_s"] = t()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        cabecalho = f.readline().rstrip("\r\n")
        amostra = [f.readline().rstrip("\r\n") for _ in range(3)]
    r["fim_de_linha_crlf"] = "\r\n" in csv_path.open("rb").read(10_000).decode("utf-8", "replace")
    r["cabecalho"] = cabecalho
    r["amostra"] = amostra
    r["colunas"] = len(cabecalho.split(","))
    r["tem_aspas"] = any('"' in linha for linha in amostra)

    spark = criar_sessao("medir_fase0")
    colunas = [c.strip('"') for c in cabecalho.split(",")]
    from pyspark.sql.types import StringType, StructField, StructType

    esquema = StructType([StructField(c, StringType()) for c in colunas])
    leitor = spark.read.option("header", True).option("mode", "FAILFAST").schema(esquema)

    t = cronometro()
    df = leitor.csv(str(csv_path))
    r["linhas"] = df.count()
    r["contar_csv_s"] = t()

    for nome, base in destinos.items():
        alvo = base / "delta"
        t = cronometro()
        df.write.format("delta").mode("overwrite").save(str(alvo))
        r[f"escrever_delta_{nome}_s"] = t()
        r[f"delta_{nome}_bytes"] = tamanho_pasta(alvo)
        r[f"delta_{nome}_arquivos"] = len(list(alvo.glob("*.parquet")))
        t = cronometro()
        n = spark.read.format("delta").load(str(alvo)).count()
        r[f"ler_delta_{nome}_s"] = t()
        assert n == r["linhas"], (nome, n, r["linhas"])
        t = cronometro()
        spark.read.format("delta").load(str(alvo)).groupBy(
            "state_code", "action_taken"
        ).count().collect()
        r[f"agregar_delta_{nome}_s"] = t()

    r["csv_bytes"] = csv_path.stat().st_size
    r["pico_memoria_container_bytes"] = pico_memoria_container()
    spark.stop()

    saida = Path("/dados/medicoes")
    saida.mkdir(parents=True, exist_ok=True)
    arq = saida / f"fase0_{zp.stem}.json"
    arq.write_text(
        json.dumps(r, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n"
    )
    csv_path.unlink()
    print(json.dumps({k: v for k, v in r.items() if k not in ("amostra", "cabecalho")}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1])
