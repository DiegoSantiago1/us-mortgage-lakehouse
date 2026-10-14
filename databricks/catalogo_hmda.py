# Databricks notebook source
# MAGIC %md
# MAGIC # HMDA no Databricks: Unity Catalog e poda de partições
# MAGIC
# MAGIC O mesmo exercício do `python -m hmda.catalogo` (docs/CATALOGO.md), agora no Databricks
# MAGIC Free Edition: a amostra de 1% da silver vira uma tabela Delta do **Unity Catalog**,
# MAGIC particionada por ano, e o `EXPLAIN` mostra que o filtro por ano lê só a pasta do ano.
# MAGIC
# MAGIC Antes de rodar: suba `amostra_silver.parquet` para o volume `workspace.hmda.arquivos`
# MAGIC (passo a passo em databricks/README.md). Os números esperados estão lá também: a
# MAGIC amostra é a mesma, então os resultados têm de ser idênticos aos do Spark local.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Catálogo "workspace" (o padrão do Free Edition) > esquema hmda > volume com o arquivo.
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.hmda
# MAGIC   COMMENT 'Lakehouse do HMDA (github.com/DiegoSantiago1/us-mortgage-lakehouse)';
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.hmda.arquivos
# MAGIC   COMMENT 'Arquivos enviados do PC (amostra da silver)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A amostra vira tabela gerenciada, particionada por ano

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.hmda.vigente_amostra
# MAGIC   PARTITIONED BY (ano)
# MAGIC   COMMENT 'Amostra de 1% por ano da silver vigente (semente 20261014)'
# MAGIC AS SELECT * FROM parquet.`/Volumes/workspace/hmda/arquivos/amostra_silver.parquet`;
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.hmda.vigente_amostra;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conferência: os mesmos números do Spark local (databricks/README.md)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, COUNT(*) AS pedidos
# MAGIC FROM workspace.hmda.vigente_amostra
# MAGIC GROUP BY ano ORDER BY ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT estado, COUNT(*) AS pedidos
# MAGIC FROM workspace.hmda.vigente_amostra
# MAGIC WHERE ano = 2022
# MAGIC GROUP BY estado ORDER BY pedidos DESC, estado LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Poda de partições: o plano
# MAGIC
# MAGIC Procure `PartitionFilters: [isnotnull(ano...), (ano... = 2022)]` no primeiro plano e a
# MAGIC ausência dele no segundo (filtro em `estado`, que não é partição).

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT estado, COUNT(*) AS pedidos
# MAGIC FROM workspace.hmda.vigente_amostra WHERE ano = 2022 GROUP BY estado;

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT ano, COUNT(*) AS pedidos
# MAGIC FROM workspace.hmda.vigente_amostra WHERE estado = 'TX' GROUP BY ano;

# COMMAND ----------

# Arquivos que cada consulta lê de verdade (mesma medição do docs/CATALOGO.md).
TABELA = "workspace.hmda.vigente_amostra"
consultas = {
    "filtro na partição (ano = 2022)": f"SELECT * FROM {TABELA} WHERE ano = 2022",
    "filtro em coluna comum (estado = 'TX')": f"SELECT * FROM {TABELA} WHERE estado = 'TX'",
    "sem filtro": f"SELECT * FROM {TABELA}",
}
for rotulo, sql in consultas.items():
    arquivos = spark.sql(sql).inputFiles()  # noqa: F821 (o Databricks cria a variável spark)
    pastas = sorted({p.split("ano=")[1][:4] for p in arquivos if "ano=" in p})
    print(f"{rotulo}: {len(arquivos)} arquivos; anos lidos: {pastas or '(sem pastas no caminho)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. O que o Unity Catalog guarda além do Delta local

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Histórico de versões (o time travel do Delta, agora no catálogo).
# MAGIC DESCRIBE HISTORY workspace.hmda.vigente_amostra;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Linhagem e permissões ficam no catálogo: Catalog Explorer > workspace > hmda >
# MAGIC -- vigente_amostra > abas "Lineage" e "Permissions".
# MAGIC SHOW TABLES IN workspace.hmda;
