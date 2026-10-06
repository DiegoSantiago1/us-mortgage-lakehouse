"""Conferência externa da gold: taxas de negativa de 2023 publicadas pelo CFPB.

    python scripts/conferir_cfpb.py

Fonte: CFPB, "Data Point: 2023 Mortgage Market Activity and Trends" (dez/2024).
Recorte do relatório: 1ª hipoteca, 1 a 4 unidades, construção no local, moradia
própria, convencional, crédito fechado, compra. Publicado: Negro 16,6%;
Hispânico branco 12,0%; Asiático 9,0%; Branco não hispânico 5,8%.

O relatório usa o dado de 2023 disponível em 2024 (provavelmente o Snapshot); a
silver tem o One Year de 2023. Então a conferência é aproximada: aceita até 0,5
ponto percentual de diferença e mostra os dois números.
"""

from pyspark.sql import functions as F

from hmda import config, tabelas
from hmda.spark import criar_sessao

PUBLICADO = {
    "Negro": 0.166,
    "Hispânico branco": 0.120,
    "Asiático": 0.090,
    "Branco não hispânico": 0.058,
}
TOLERANCIA = 0.005

cfg = config.carregar()
spark = criar_sessao("cfpb", driver_memory=cfg.driver_memory)
s = spark.read.format("delta").load(tabelas.caminho(cfg.tabelas, tabelas.SILVER)).where("ano = 2023")
recorte = s.where(
    (F.col("prioridade_garantia") == 1)
    & F.col("unidades").isin("1", "2", "3", "4")
    & (F.col("metodo_construcao") == 1)
    & (F.col("ocupacao") == 1)
    & (F.col("tipo_emprestimo") == 1)
    & (F.col("linha_de_credito") == False)  # noqa: E712
    & (F.col("finalidade") == 1)
    & F.col("resultado").isin(1, 2, 3)
)
grupo = (
    F.when((F.col("raca") == "White") & (F.col("etnia") == "Hispanic or Latino"), "Hispânico branco")
    .when((F.col("raca") == "White") & (F.col("etnia") == "Not Hispanic or Latino"), "Branco não hispânico")
    .when(F.col("raca") == "Black or African American", "Negro")
    .when(F.col("raca") == "Asian", "Asiático")
)
linhas = (
    recorte.withColumn("grupo", grupo)
    .where(F.col("grupo").isNotNull())
    .groupBy("grupo")
    .agg(F.count("*").alias("pedidos"), F.avg((F.col("resultado") == 3).cast("double")).alias("taxa"))
    .collect()
)
falhas = 0
for r in sorted(linhas, key=lambda x: x["grupo"]):
    pub = PUBLICADO[r["grupo"]]
    dif = r["taxa"] - pub
    ok = abs(dif) <= TOLERANCIA
    falhas += not ok
    print(
        f"{'ok   ' if ok else 'FALHA'} {r['grupo']:<22} silver {r['taxa']:.2%}  CFPB {pub:.1%}  "
        f"diferença {dif * 100:+.2f} p.p.  ({r['pedidos']:,} pedidos)"
    )
spark.stop()
raise SystemExit(1 if falhas else 0)
