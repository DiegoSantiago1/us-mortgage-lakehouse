"""Roda no build da imagem: baixa os jars do Delta para o cache do Ivy
e prova que escrever e ler uma tabela Delta funciona."""

import tempfile

from hmda.spark import criar_sessao

spark = criar_sessao("aquecer", driver_memory="1g", nucleos="1")
with tempfile.TemporaryDirectory() as pasta:
    spark.range(3).write.format("delta").save(pasta + "/t")
    assert spark.read.format("delta").load(pasta + "/t").count() == 3
print("delta ok:", spark.version)
spark.stop()
