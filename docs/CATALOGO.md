# Catálogo de tabelas e poda de partições (gerado por `python -m hmda.catalogo`)

Tabelas registradas no metastore Hive local (externas: o catálogo guarda nome, esquema e local; os dados ficam no Delta):

- `bronze.pedidos`
- `controle.cargas`
- `controle.silver_versoes`
- `dq.checagens`
- `gold.equidade`
- `gold.impacto`
- `gold.mercado`
- `gold.motivos`
- `gold.negativas`
- `gold.preco_renda`
- `revisoes.campos`
- `revisoes.diferencas`
- `revisoes.instituicoes`
- `silver.vigente`

`silver.vigente`: 109 arquivos Parquet, 4.8 GB, particionada por `ano`.

| Consulta | Arquivos lidos | Pastas de partição lidas |
|---|---|---|
| filtro na partição (ano = 2022) | 14 de 109 (13%) | 1: ano=2022 |
| filtro em coluna comum (estado = 'TX') | 109 de 109 (100%) | 8: ano=2018, ano=2019, ano=2020, ano=2021, ano=2022, ano=2023, ano=2024, ano=2025 |
| sem filtro | 109 de 109 (100%) | 8: ano=2018, ano=2019, ano=2020, ano=2021, ano=2022, ano=2023, ano=2024, ano=2025 |

## O que o plano mostra

**filtro na partição (ano = 2022)**

```
PartitionFilters: [isnotnull(ano#14389), (ano#14389 = 2022)]
ReadSchema: struct<estado:string>
```

**filtro em coluna comum (estado = 'TX')**

```
(nenhum filtro de partição: lê todas as pastas)
ReadSchema: struct<estado:string>
```

**sem filtro**

```
(nenhum filtro de partição: lê todas as pastas)
ReadSchema: struct<>
```

## Como ler

- **Poda de partições:** `ano` é a coluna de partição, então cada ano é uma pasta (`ano=2022/`). Com `WHERE ano = 2022`, o Spark escolhe as pastas antes de abrir qualquer arquivo (`PartitionFilters` no plano).
- **Poda de colunas:** `ReadSchema` lista só as colunas que a consulta usa. O Parquet guarda cada coluna separada, então as outras nem saem do disco.
- **Filtro em coluna comum:** `estado` não é partição e todo arquivo tem pedidos de todos os estados, então o mínimo e o máximo guardados pelo Delta não deixam pular nenhum arquivo. Ordenar os dados por estado dentro de cada ano (Z-ORDER ou liquid clustering) faria esse filtro pular arquivos também; não foi feito porque as análises daqui agregam o país inteiro.

## Plano completo: filtro na partição (ano = 2022)

```sql
SELECT estado, COUNT(*) AS pedidos FROM silver.vigente WHERE ano = 2022 GROUP BY estado
```

```
== Physical Plan ==
AdaptiveSparkPlan (6)
+- HashAggregate (5)
   +- Exchange (4)
      +- HashAggregate (3)
         +- Project (2)
            +- Scan parquet spark_catalog.silver.vigente (1)


(1) Scan parquet spark_catalog.silver.vigente
Output [2]: [estado#14392, ano#14389]
Batched: true
Location: PreparedDeltaFileIndex [file:/lake/tabelas/silver/vigente]
PartitionFilters: [isnotnull(ano#14389), (ano#14389 = 2022)]
ReadSchema: struct<estado:string>

(2) Project
Output [1]: [estado#14392]
Input [2]: [estado#14392, ano#14389]

(3) HashAggregate
Input [1]: [estado#14392]
Keys [1]: [estado#14392]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#14534L]
Results [2]: [estado#14392, count#14535L]

(4) Exchange
Input [2]: [estado#14392, count#14535L]
Arguments: hashpartitioning(estado#14392, 48), ENSURE_REQUIREMENTS, [plan_id=1506]

(5) HashAggregate
Input [2]: [estado#14392, count#14535L]
Keys [1]: [estado#14392]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#14452L]
Results [2]: [estado#14392, count(1)#14452L AS pedidos#14384L]

(6) AdaptiveSparkPlan
Output [2]: [estado#14392, pedidos#14384L]
Arguments: isFinalPlan=false
```
