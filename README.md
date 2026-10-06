# US Mortgage Lakehouse (HMDA)

A PySpark + Delta Lake lakehouse over **222.7 million US mortgage records** (HMDA, 2018–2025): who gets a home loan, where homes got least affordable, what the 2022 rate hike did, and **how much the government's own data changes** between the three versions it publishes for every year. Every number is checked against official totals before it can be published.

🇧🇷 [Leia em português](README.pt-BR.md)

## 🔗 Access the project

[![Open the project](https://img.shields.io/badge/%E2%96%B6%20Open%20the%20project-EA580C?style=for-the-badge)](https://diegosantiago1.github.io/us-mortgage-lakehouse/)
[![See it in my portfolio](https://img.shields.io/badge/See%20it%20in%20my%20portfolio-1F2937?style=for-the-badge&logo=googlechrome&logoColor=white)](https://diegosantiago1.github.io/Portifolio/#projetos)

**Direct link:** https://diegosantiago1.github.io/us-mortgage-lakehouse/ (opens the results page: a map of the US, rates, denials, fairness and the revisions chapter, in English or Portuguese, light or dark).

<!-- RESULTADOS -->

## The problem

Every year about 5,000 US lenders must report **every mortgage application** they receive under the Home Mortgage Disclosure Act: amount, income, property, location, outcome, denial reasons, interest rate and demographics. The government publishes it row by row: 26 million records for 2021 alone.

Two problems live in this data:

1. **Business:** where did homes get most expensive relative to income? What did the 2022 rate hike do to purchases and refinancing? Who gets denied and why? Do applicants with similar profiles get similar answers?
2. **Engineering:** the same year is published **three times** (Snapshot, One Year, Three Year) as lenders resubmit and correct. The final version arrives almost three years later. The public file **has no loan identifier** (removed for privacy), so you cannot compare versions with a `MERGE` on a key.

I work at a car dealership in Recife, Brazil, where I see auto financing approved and denied every day. I wanted to understand the same problem at national scale, with the largest public credit dataset there is.

## Architecture

```
FFIEC files (12 zips, 12 GB) ──► manifest (URL, ETag, SHA-256) ──► C:\dados\hmda   (raw, kept untouched)
                                                                        │
BRONZE   all text + load metadata, partitioned by (year, version)   ◄───┘   222.7M rows, 0 malformed
   │        idempotent (SHA-256), replaceWhere per partition, RESTORE if counts don't match
   ├──► REVISIONS   multiset difference by row hash (96-bit), per lender and per field
   ▼
SILVER   current version of each year, typed; NA/Exempt -> null (never zero); 9 CHECK constraints
   │        versions applied in the government's publication order -> each one is a Delta commit (time travel)
   ▼
QUALITY  blocking: state × outcome = FFIEC API, records per lender = Transmittal Sheet, rows conserved
   ▼
GOLD     price/income, market & rates, denials & reasons, fairness (observed ÷ expected), impact of revisions
   ▼
PAGE     aggregated JSON -> static site (D3, US map, EN/PT) on GitHub Pages
```

Everything runs locally in Docker (Python 3.13, Java 21, PySpark 4.2.0, delta-spark 4.4.1, jars baked into the image, so it runs with `--network none`). One command per step, no orchestrator: the pipeline runs a few times a year, when the government publishes a new version.

## Engineering highlights

| What | How |
|---|---|
| Idempotent ingestion | A file already loaded (same SHA-256) is skipped; a republished file replaces only its own partition (`replaceWhere`). Downloads resume with HTTP Range. |
| Nothing lost silently | Lines are counted while unzipping, independently of Spark. If the Delta table holds a different number, it is rolled back with `RESTORE`. 222,705,887 rows in, 222,705,887 rows stored, 0 malformed. |
| Change data without a key | Each row becomes a 96-bit fingerprint (xxhash64 + murmur3 of its 99 text columns, with a null marker). Versions are compared as **multisets** (identical duplicate rows are legitimate). Every run is cross-checked against Spark's exact `exceptAll` on a whole state. |
| "Which field was corrected?" | One hash per column combined with XOR: "the row without column X" = total XOR hash(X). The first version (99 hashes of 98 columns) ran the JVM out of memory on a 3-row test. |
| Quality gates | State × outcome matched against the FFIEC Data Browser API, cell by cell; records per lender matched against the Transmittal Sheet (~4,400 lenders). If anything fails, gold and the page refuse to build. |
| Time travel with meaning | The silver table receives each government version in **publication order** (freeze dates read from the FFIEC site's source), so `VERSION AS OF` answers "what did an analyst see in May 2023?". |
| CHECK constraints | 9 domain constraints on the Delta table; a test proves they block even direct writes that bypass the pipeline. |

## What the real data taught me (bugs found and fixed)

- **303,002 false rejections in 2018.** My first rules rejected rows the government publishes and counts: exempt lenders report denial reason `1111`; some rates are unreadable as `decimal(8,3)` (an interest rate of 260,000%); an applicant age of `9999`. The rejected-rows table, which keeps the original text, showed the cause in minutes. New rule: a readable number, even absurd, becomes an **alert**, never a rejection.
- **A slow silver layer (1,900 rows/s per core).** I repeated the same cleaning inside every rule, and the generated code grew until Spark fell back to interpreted evaluation. On top of that, lambda functions (`exists`, `array_compact`) always run interpreted in Spark. Staging the transform (clean → type → validate) and replacing those with `OR`/`concat_ws` took a year from 803 s to 548 s.
- **A download resume that mixed two files.** If the connection dropped and the government replaced the file before the retry, the Range request glued the old start to the new end. With equal sizes, the corrupt zip passed. Fixed by storing the ETag next to the partial file; a test reproduces it.

## Run it

```bash
# 1. Image (on a machine with an HTTPS-inspecting antivirus, pass its root CA as a build secret)
docker compose build
# 2. Pipeline (each step is idempotent)
docker compose run --rm spark python -m hmda.baixar --escopo      # 12 files, ~12 GB
docker compose run --rm spark python -m hmda.bronze --escopo      # ~80 min
docker compose run --rm spark python -m hmda.silver --escopo
docker compose run --rm spark python -m hmda.dq                   # exits 1 if anything fails
docker compose run --rm spark python -m hmda.revisoes
docker compose run --rm spark python -m hmda.gold
docker compose run --rm spark python -m hmda.exportar_site
# 3. Tests
docker compose run --rm spark pytest
```

## Glossary (code is in Portuguese)

| Portuguese | English |
|---|---|
| `bronze/pedidos`, `silver/vigente`, `silver/rejeitados` | raw applications, current (latest version) typed table, rejected rows |
| `controle/cargas`, `controle/silver_versoes` | load log, which Delta commit is which government version |
| `revisoes/diferencas`, `revisoes/instituicoes`, `revisoes/campos` | row-level multiset diff, per lender, per corrected field |
| `gold/preco_renda`, `mercado`, `negativas`, `motivos`, `equidade`, `impacto` | price/income, market & rates, denials, denial reasons, fairness, impact of revisions |
| `resultado`, `finalidade`, `renda_milhares`, `valor_imovel`, `taxa_juros` | action taken, loan purpose, income (thousands), property value, interest rate |
| `alertas`, `tem_isencao` | alerts (kept rows with odd values), row has an Exempt field |
| `baixar`, `carregar`, `aplicar`, `conferir` | download, load, apply, check |

## Limitations

- The public data has **no credit score or assets**. The fairness chapter controls for income, debt-to-income, loan-to-value, loan amount, loan type and state, and says plainly that a gap does not prove discrimination.
- Without a loan identifier, a corrected row appears as "one left, one entered". The field analysis counts pairs that differ in exactly one column; it does not claim to know which row became which.
- The official API only answers for the latest version of each year (and only rows with a state), so older versions are checked internally, not against the government.
- The data is reported by lenders; values like a 260,000% interest rate are kept and flagged, and the analyses filter them.

## Decisions

Every decision, with the measurement behind it and the alternative I rejected, is in [docs/DECISOES.md](docs/DECISOES.md) (in Portuguese). The plan is in [docs/PLAN.md](docs/PLAN.md).

## Data and license

Source: FFIEC / CFPB, HMDA Modified LAR and Transmittal Sheet (https://ffiec.cfpb.gov/data-publication/). The official catalog lists no explicit license; the data is published by law for public use. Only aggregates are published here.

---

Diego Freitas Santiago · [GitHub](https://github.com/DiegoSantiago1) · [LinkedIn](https://www.linkedin.com/in/diego-freitas-santiago) · [Portfolio](https://diegosantiago1.github.io/Portifolio/)
