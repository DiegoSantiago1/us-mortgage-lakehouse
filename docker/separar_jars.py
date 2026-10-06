"""Roda no build: copia do cache do Ivy só os jars que o PySpark ainda não tem.

Os outros (Hadoop, Parquet, Jackson, log4j...) já vêm no PySpark; duplicá-los
só aumentaria a imagem e criaria o risco de duas versões da mesma classe.
"""

import re
import shutil
import sys
from pathlib import Path

import pyspark

VERSAO = re.compile(r"-\d[\w.\-]*\.jar$")


def artefato(nome: str) -> str:
    return VERSAO.sub("", nome)


ivy, destino = Path(sys.argv[1]), Path(sys.argv[2])
do_spark = {artefato(j.name) for j in (Path(pyspark.__file__).parent / "jars").glob("*.jar")}
destino.mkdir(parents=True, exist_ok=True)
copiados = 0
for jar in sorted(ivy.glob("*.jar")):
    # No cache do Ivy o nome é "grupo_artefato-versao.jar".
    if artefato(jar.name.split("_", 1)[1]) not in do_spark:
        shutil.copy2(jar, destino / jar.name)
        copiados += 1
print(f"{copiados} jars em {destino}")
