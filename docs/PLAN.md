# PLAN — us-mortgage-lakehouse

## Estado (06/10/2026)

| Fase | Situação |
|---|---|
| Grill | Feito (06/10/2026); respostas do Diego em [DECISOES.md](DECISOES.md) |
| 0 Ambiente e medição | Feita (06/10/2026): imagem Docker, download idempotente, 2021 medido, escopo decidido (D19–D26) |
| 1 Ingestão (bronze) | Não começada |
| 2 Silver tipada + qualidade | Não começada |
| 3 Versões e revisões (CDC por hash) | Não começada |
| 4 Gold e análises | Não começada |
| 5 Página de resultados | Não começada |
| 6 Fechamento (README, CI, revisões) | Não começada |
| 7 Databricks Free (opcional) | Não começada |

> Plano escrito em 06/10/2026, depois do grill. Números marcados com **(medido)** foram medidos; os marcados com **(oficial)** vêm da documentação do FFIEC/CFPB; **(estimativa)** ainda precisa ser medido. Itens marcados com **[a conferir]** dependem da fase 0.

## 1. Problema

Todo ano, cerca de 5 mil instituições financeiras dos EUA são obrigadas por lei (Home Mortgage Disclosure Act, HMDA) a informar **cada pedido de financiamento imobiliário** que receberam: valor, renda, imóvel, localização, resultado (aprovado, negado, desistência), motivo da negativa, taxa de juros e dados demográficos do solicitante. O governo publica isso linha a linha: em 2023, ~10 milhões de pedidos e um arquivo nacional de ~5,8 GB **(oficial)**.

Dois problemas reais aparecem nesse dado:

1. **Negócio:** onde a casa ficou mais cara em relação à renda? O que a alta dos juros de 2022 fez com a compra e com o refinanciamento? Quem tem o crédito negado e por quê? Grupos com renda, dívida e valor parecidos têm a mesma taxa de negativa?
2. **Engenharia:** o mesmo ano é publicado **várias vezes** (Snapshot, One Year, Three Year), porque os bancos reenviam e corrigem os dados. A versão final só sai ~3 anos depois. **O dado público não tem identificador do empréstimo** (o ULI foi removido por privacidade), então não dá para comparar versões com um `MERGE` por chave. Quem analisa a versão preliminar não sabe o quanto ela vai mudar.

**Solução:** um lakehouse local (PySpark + Delta Lake em Docker) em camadas bronze → silver → gold. Ele carrega cada versão de forma idempotente e detecta o que mudou entre versões por **hash da linha com multiplicidade**, mantendo o histórico com o time travel do Delta. As análises são conferidas contra os números oficiais e publicadas numa página com mapa dos EUA.

**Por que o Diego (narrativa de entrevista):** na concessionária ele vê, todo dia, financiamento de veículo aprovado e recusado. Ele quis entender esse mesmo problema em escala nacional, com o dado público mais completo que existe sobre crédito.

## 2. O que o projeto prova (para recrutador e entrevista)

| Habilidade | Onde aparece |
|---|---|
| Spark (PySpark) | todas as camadas: leitura de CSV grande, tipagem, agregações, janelas |
| Lakehouse / Delta Lake | tabelas Delta com transação ACID, `replaceWhere`, CHECK constraints, histórico e time travel |
| Arquitetura Medallion | bronze (texto + metadados) → silver (tipada e validada) → gold (agregados para análise) |
| Carga incremental e idempotente | manifesto com hash do arquivo: recarregar não duplica; arquivo novo substitui só a sua partição |
| CDC sem chave | diferença entre versões por hash da linha com multiplicidade; revisões por instituição |
| Qualidade de dados | checagens bloqueantes + conferência contra os totais oficiais do governo |
| Análise | preço/renda por estado, efeito dos juros, negativas e motivos, equidade com controles |
| Visualização | página própria com mapa dos EUA, EN/PT, claro/escuro, celular |
| Engenharia de software | Docker, pytest com Spark, CI no GitHub Actions, ruff, decisões documentadas |

## 3. Dados

### 3.1 Fonte **(oficial, conferido em 06/10/2026)**
- FFIEC/CFPB, HMDA Modified LAR / Dataset nacional. Fonte das versões: ffiec.cfpb.gov/documentation/faq/static-dataset-faq.
- Versões do mesmo ano:
  - Snapshot (~1 mês depois do prazo);
  - One Year (≥ 12 meses);
  - Three Year (≥ 34 meses, a final).
- Disponíveis:
  - Three Year: 2017–2022;
  - One Year: 2019–2024 (não há One Year de 2017 e 2018);
  - Snapshot: 2017–2025.
- Sem ULI; 27 campos retirados e 6 modificados no dado público (valores arredondados, idade em faixas etc.).
- Sem score de crédito.
- **Conferido na fase 0:**
  - **Licença:** o catálogo oficial (catalog.data.gov, "HMDA Public Data (Starting in 2017)") não traz licença explícita. O dado é publicado por obrigação legal para uso público, e o que é feito por servidores federais é "U.S. Government Work" (sem copyright nos EUA). O README cita a fonte e diz exatamente isso, sem afirmar mais.
  - **Arquivos:** `https://files.ffiec.cfpb.gov/static-data/{snapshot|one-year|three-year}/{ano}/...` (lido no código do site oficial, `cfpb/hmda-frontend`). São zips com um CSV (também há versão separada por `|`) e um lixo do macOS (`__MACOSX/`, D23). O CSV não tem aspas e usa fim de linha LF, com 99 colunas **(medido, 2021)**.
  - **Totais oficiais:** `https://ffiec.cfpb.gov/v2/data-browser-api/view/aggregations` (contagem e soma por filtro). Usa a versão mais recente de cada ano (D24). Para 2021 Three Year, a soma por estado bateu **exatamente** com o arquivo: 26.098.241 linhas com estado, mais 171.739 sem estado (`NA`), que a API não conta **(medido)**.

### 3.2 Campos que importam (conferir códigos no dicionário oficial na fase 2)
| Campo | Uso |
|---|---|
| `activity_year`, `lei` | ano e instituição (o LEI identifica o banco, não o empréstimo) |
| `state_code`, `county_code`, `derived_msa_md`, `census_tract` | geografia (mapa por estado, região metropolitana e condado) |
| `action_taken` | 1 originado, 2 aprovado e não aceito, 3 negado, 4 desistência, 5 incompleto, 6 comprado, 7–8 pré-aprovação |
| `loan_purpose` | 1 compra, 2 reforma, 31 refinanciamento, 32 refinanciamento com saque, 4 outro |
| `loan_type`, `lien_status`, `occupancy_type` | recorte comparável (convencional/FHA/VA, 1ª hipoteca, moradia própria) |
| `loan_amount`, `property_value`, `income` | valor (ponto médio de faixa de US$ 10 mil), renda (em milhares) |
| `debt_to_income_ratio`, `combined_loan_to_value_ratio` | DTI em faixas ("<20%", "36", ">60%"…), LTV |
| `interest_rate` | juros (só em empréstimos originados) |
| `denial_reason_1..4` | 1 DTI, 2 emprego, 3 histórico de crédito, 4 garantia, 5 caixa, 6 informação não verificável, 7 incompleto, 8 seguro, 9 outro |
| `derived_race`, `derived_ethnicity`, `derived_sex` | equidade |

Valores especiais que a silver precisa tratar: `Exempt` (instituição dispensada daquele campo), `NA`, vazio e códigos "não se aplica" (ex.: 1111, 8888). Nenhum deles pode virar zero.

### 3.3 Escopo de anos e versões (decidido com a medição, D26)
- **Começar em 2018:** a regra de 2018 mudou os campos (DTI, LTV, juros e motivos passam a ser padronizados). 2017 tem outro formato e fica fora (D8).
- **Série histórica** (uma versão por ano, a mais madura disponível = "vigente"):
  - Three Year para 2018–2022;
  - One Year para 2023–2024;
  - Snapshot para 2025, marcado como preliminar.
- **Versões para o CDC** (prioridade do Diego, D7): as três versões de 2021 e de 2022 (Snapshot → One Year → Three Year). Se o disco permitir, mais anos.
- Ficaram as três versões de 2021 e 2022, sem anos extras: 12 arquivos no total (`fontes.escopo()`).

### 3.4 Medição da fase 0 (2021 Three Year, o maior ano; `docs/medicoes/`)
| Medida | Valor **(medido)** |
|---|---|
| Linhas / colunas | 26.269.980 / 99 |
| Zip / CSV / Delta só com texto | 1,49 GB / 9,96 GB / 1,21 GB (75 arquivos) |
| Download | ~13 MiB/s (~2 min por zip) |
| Extrair o zip (Python `zipfile`) | 291 s |
| Contar o CSV (Spark, esquema explícito) | 34 s |
| CSV → Delta: pasta do Windows × volume Docker | 393 s × 288 s |
| Ler (count) / agregar estado × resultado | 6,1 s e 6,4 s × 2,6 s e 3,0 s |
| Pico de memória do container | 8,5 GB (inclui o cache de disco da extração) |

Conta do escopo **(estimativa a partir da medição)**: ~225 mi linhas na bronze ≈ 10 GB, zips ≈ 12,4 GB, silver vigente ≈ 5 GB, CSV temporário até 10 GB. Cabe nos 55 GB livres.

## 4. Arquitetura

```
FFIEC (download)            C:\dados\hmda  (pasta do Windows, fora do OneDrive) = o que precisa durar
      │                     ├── bruto\{ano}\{versao}\<nome>_<sha12>.zip   (intocado, D22)
      ▼                     └── manifesto.json
 baixar ──► manifesto       volume Docker `lake` (/lake) = derivado, 2x mais rápido (D25)
      │                     ├── tabelas/  (Delta)
      │                     └── tmp/      (CSV extraído durante a carga, apagado depois)
      │
      ▼
 BRONZE  bronze.pedidos        TODAS as versões; tudo texto + metadados (ano, versao, arquivo, sha256, carregado_em)
      │                        partição (ano, versao); recarga = replaceWhere da partição
      ├──► REVISÕES  revisoes.diferencas      (ano, de_versao, para_versao, hash, +n / −n), sobre o texto original
      │              revisoes.por_instituicao  (lei: linhas que entraram, saíram, saldo)
      ▼
 SILVER  silver.vigente        SÓ a versão mais madura de cada ano, tipada, CHECK constraints do Delta;
      │                        recebe as versões em ordem: cada troca = um commit (time travel = versões antigas tipadas)
      │  dq.checagens          resultado de cada checagem por carga (falhou = carga não publica)
      ▼
 GOLD    gold.*                agregados por ano × estado/MSA/condado × finalidade × resultado
      │
      ▼
 exportar_site ──► site/dados/*.json ──► página (GitHub Pages)
```

### 4.1 Decisões de forma (detalhes em DECISOES.md)
- **Bronze guarda tudo como texto** (D11): nenhuma linha se perde por erro de tipo, e o hash é calculado sobre o texto original normalizado.
- **Hash da linha com multiplicidade** (D9): linhas idênticas podem repetir legitimamente (sem ULI, dois empréstimos iguais na mesma região são indistinguíveis). A diferença entre versões é calculada por contagem: `entrou = max(0, n_nova − n_antiga)` e `saiu = max(0, n_antiga − n_nova)`, por hash. Uma linha corrigida aparece como "saiu uma, entrou outra". O projeto não afirma saber qual linha virou qual; mostra o volume de revisão por instituição, estado e campo.
- **Duas noções de "versão"** (D10), sempre separadas:
  - a versão **do governo** (Snapshot/One Year/Three Year), que é dado na coluna `versao`;
  - a versão **da tabela Delta** (commit), que é o histórico da nossa carga.

  `silver.vigente` é atualizada versão a versão do governo, em ordem. Com time travel (`VERSION AS OF`), dá para responder "o que um analista via em 2023, quando só existia o Snapshot de 2022?".
- **Tabelas por caminho**, sem metastore (D16): um módulo `tabelas.py` mapeia nome → caminho. Menos uma peça para manter localmente; no Databricks vira Unity Catalog.
- **Comandos por etapa, sem Airflow** (D3):

  ```
  docker compose run --rm spark python -m hmda.baixar --ano 2022 --versao three_year
  ```

  O mesmo vale para `hmda.bronze`, `hmda.silver`, `hmda.revisoes`, `hmda.gold` e `hmda.exportar_site`. Cada comando é idempotente.

### 4.2 Ambiente
- Imagem Docker própria (D15, D19, D20): Python 3.13, Java 21, PySpark 4.2.0 e delta-spark 4.4.1, com os 11 jars do Delta embutidos. O Spark roda sem internet (testado com `--network none`). Build numa máquina com Norton: `docker build --secret id=ca,src=C:/dados/hmda/certs/norton-ca.pem -t us-mortgage-lakehouse:local .`
- Memória: `.wslconfig` com 10 GB (feito; o Docker vê 9,7 GiB). Driver do Spark com 6 GB (`SPARK_DRIVER_MEMORY`), `local[*]` nos 12 núcleos.
- Pacote Python em `src/hmda/`, código em português (D5), testes em `tests/` com `SparkSession` compartilhada e dados de exemplo pequenos, criados à mão com os casos difíceis (sem download no teste).
- Qualidade de código: ruff e pytest; mypy só onde os tipos do PySpark ajudam (decidir na fase 1).

## 5. Análises (fase 4)

Todas com recorte comparável explícito (ex.: compra, 1ª hipoteca, moradia própria, unidade de 1–4 famílias) e conferidas contra os totais oficiais.

1. **Preço em relação à renda** por estado e região metropolitana, 2018–2025: mediana de `property_value / (income × 1000)` dos empréstimos originados para compra.
2. **Alta dos juros de 2022:** volume de compra × refinanciamento e juros medianos por estado, 2020 → 2023. O refinanciamento deve despencar mais que a compra (a confirmar com o dado).
3. **Negativas e motivos:** taxa de negativa (denominador: originado + aprovado não aceito + negado **[conferir a convenção do CFPB]**) por estado, finalidade e tipo de empréstimo; motivos mais citados.
4. **Equidade, com cuidado** (D4):
   - taxa de negativa por raça/etnia e sexo, **padronizada** dentro de células de mesma faixa de renda, DTI, LTV, valor, tipo de empréstimo e estado;
   - publicar só grupo com n mínimo (proposta: célula ≥ 30 pedidos, grupo ≥ 1.000 no recorte);
   - texto fixo na página: "sem score de crédito; diferença não prova discriminação".
5. **Quanto o dado do governo muda:** de Snapshot para Three Year, quantas linhas entram e saem, em quais instituições e estados, e quanto isso move as métricas das perguntas 1 a 4.

**Conferência (bloqueante):** total de pedidos por ano × versão × `action_taken` (e por estado) igual ao publicado pelo FFIEC (API do Data Browser ou relatórios oficiais **[a conferir]**). Se não bater, a gold não é publicada e o exportador se recusa, como no Projeto 3.

## 6. Página de resultados (regra do Diego)

- Página própria em capítulos, no padrão do Projeto 3:
  - EN/PT, abrindo no idioma do navegador;
  - claro/escuro;
  - funciona no celular;
  - visual próprio (não reaproveitar a paleta dos outros projetos).
- Capítulos (rascunho):
  1. o problema;
  2. mapa do preço em relação à renda;
  3. os juros de 2022;
  4. quem tem o crédito negado e por quê;
  5. equidade, com as ressalvas;
  6. "o dado do governo muda" (o capítulo de engenharia, com diagrama do lakehouse);
  7. como foi feito.
- Mapa dos EUA: SVG com d3-geo + us-atlas (geometria do Census) por CDN permitido. Decidir na fase 5 entre isso e uma alternativa mais leve (D18).
- Dados da página: JSON pequeno exportado da gold (nunca linha a linha).
- Publicação: GitHub Pages via Actions, quando o Diego autorizar o repositório público. Link no README ("Acessar o projeto"), no card do portfólio e no README de perfil.

## 7. Fases e horas (estimativa 74 h; corte se passar de 85 h)

| Fase | Entrega | Pronto quando | h |
|---|---|---|---|
| 0 | `C:\dados\hmda`, `.wslconfig`, Dockerfile, conferir licença, formatos, URLs e versões compatíveis; **medir um ano** (download, tamanho, linhas, tempo CSV→Delta, tamanho Delta, pico de memória, bind mount × volume) | tabela de medições neste PLAN + escopo de anos/versões decidido com o Diego | 6 |
| 1 | `baixar` + manifesto + bronze idempotente | recarregar o mesmo arquivo não muda nada (testado); arquivo trocado substitui só a partição; testes com arquivo truncado, cabeçalho diferente, linha com coluna a mais | 8 |
| 2 | silver tipada, valores especiais, CHECK constraints, schema `dq` | checagens bloqueantes rodando; inserir linha hostil viola o CHECK (testado); contagem bronze = silver + rejeitadas | 12 |
| 3 | revisões por hash com multiplicidade, por instituição; `silver.vigente`; time travel demonstrado | teste com duplicatas legítimas e linha corrigida; números de 2021/2022 entre versões | 10 |
| 4 | gold + as 5 análises + conferência com os totais oficiais | cada número citado conferido e rodado de novo | 14 |
| 5 | página com mapa, EN/PT, claro/escuro | conferida no celular e no desktop (Playwright, captura de viewport) | 12 |
| 6 | README EN/PT (com glossário PT→EN), DECISOES, CI com Spark, três revisões (engenharia, QA, dados) | CI verde; revisões sem pendência aberta | 8 |
| 7 | Databricks Free (opcional) | mesmo código rodando num ano de amostra | 4 |

Ordem de corte se apertar: fase 7 → condados (fica estado + MSA) → anos extras no CDC.

Ritmo: o Diego escolheu fazer **só o Projeto 6 até terminar** (D13). A 15–20 h por semana, são 4 a 5 semanas, com término previsto para o início ou meados de novembro, perto do REC'n'Play.

## 8. Riscos

| # | Risco | Mitigação |
|---|---|---|
| R1 | Spark no Docker escrevendo em pasta do Windows (bind mount) é lento no WSL2 | **medido e resolvido (D25):** tabelas no volume Docker (2x mais rápido para ler); só o bruto fica em `C:\dados\hmda` |
| R2 | O disco do Docker (VHDX) cresce e não encolhe sozinho | apagar o CSV depois de conferir; acompanhar `docker system df`; documentar como compactar |
| R3 | Falta de memória (OOM) no Spark com 5–6 GB de CSV | 10 GB no WSL, leitura com schema explícito (sem `inferSchema`), memória do driver medida |
| R4 | Formato ou colunas mudam entre anos e versões | schema esperado por ano; checagem de cabeçalho bloqueante na bronze |
| R5 | Análise de equidade mal interpretada | recorte comparável, n mínimo, texto de ressalva fixo, revisão de dados antes de publicar |
| R6 | Curva de aprendizado de Spark + Delta | só essas duas tecnologias novas; nada de Airflow, Kafka ou nuvem neste projeto |
| R7 | Chegar ao REC'n'Play sem as sessões de explicação dos Projetos 2 a 5 | decisão do Diego (D13); a seção de aprendizado de cada fase deste projeto é feita na hora, para não acumular dívida aqui também |
| R8 | Número oficial para conferir não estar disponível no formato esperado | conferir a API na fase 0; se não houver, usar os relatórios publicados e dizer isso no README |

## 9. Fora do escopo (de propósito)
- Airflow, streaming, Kafka (D3).
- Nuvem paga e AWS: a AWS fica na fase 7 do Projeto 4.
- dbt: já está no Projeto 4; aqui a transformação é em PySpark, que é o que o projeto quer provar.
- Modelo de machine learning de aprovação de crédito: sem score de crédito seria enganoso, e um modelo que "decide crédito" por raça ou sexo é eticamente delicado.
- Dados do Brasil (não há base nacional aberta de transações).

## 10. O que o projeto deve permitir responder em entrevista
- Por que Spark para ~10 milhões de linhas por ano? (Resposta honesta com número: ver D17, comparação opcional com DuckDB.)
- O que o Delta Lake dá que o Parquet sozinho não dá? (transação, `replaceWhere`, CHECK, histórico, time travel)
- Como garantir que rodar a carga duas vezes não duplica nada?
- Como comparar duas versões de um dado sem chave primária? Por que a multiplicidade importa?
- Qual a diferença entre a versão do governo e a versão da tabela Delta?
- Por que particionar por ano e versão, e não por estado?
- O que acontece se o arquivo do governo mudar de formato?
- Como você sabe que os seus números estão certos?
- A taxa de negativa maior para um grupo prova discriminação? O que falta no dado?
- Como isso rodaria no Databricks ou num cluster de verdade?
