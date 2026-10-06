"""Mede como a silver escala com o número de núcleos e quanto tempo vai em GC.

    python scripts/medir_escala.py <nucleos> <driver_memory>
"""

import json
import sys
import tempfile
import time
import urllib.request

from pyspark.sql import functions as F

from hmda import silver
from hmda.spark import criar_sessao


def gc_total(spark) -> tuple[int, int]:
    base = spark.sparkContext.uiWebUrl + "/api/v1/applications"
    app = json.load(urllib.request.urlopen(base))[0]["id"]
    ex = json.load(urllib.request.urlopen(f"{base}/{app}/executors"))[0]
    return ex["totalGCTime"], ex["totalDuration"]


nucleos, memoria = sys.argv[1], sys.argv[2]
extras = {"spark.databricks.delta.stats.collect": "false"} if len(sys.argv) > 3 else {}
spark = criar_sessao("escala", driver_memory=memoria, nucleos=nucleos, extras=extras)
bronze = (
    spark.read.format("delta")
    .load("/lake/tabelas/bronze/pedidos")
    .where("_ano = 2018 AND state_code IN ('CA', 'TX', 'FL')")
)
df = silver.transformar(bronze, com_bruto=False).where(F.col("_rejeicoes") == "")
gc0, dur0 = gc_total(spark)
t = time.monotonic()
with tempfile.TemporaryDirectory(dir="/lake") as pasta:
    df.drop("_rejeicoes").write.format("delta").save(pasta + "/t")
    linhas = spark.read.format("delta").load(pasta + "/t").count()
segundos = time.monotonic() - t
gc1, dur1 = gc_total(spark)
print(
    f"nucleos={nucleos} memoria={memoria} extras={extras}: {linhas:,} linhas em {segundos:.0f} s "
    f"({linhas / segundos:,.0f} linhas/s); GC {(gc1 - gc0) / 1000:.0f} s de "
    f"{(dur1 - dur0) / 1000:.0f} s de tarefa"
)
spark.stop()
