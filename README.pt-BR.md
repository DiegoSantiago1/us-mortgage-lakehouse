# Lakehouse do financiamento imobiliário dos EUA (HMDA)

Um lakehouse em PySpark + Delta Lake com **222,7 milhões de registros de financiamento imobiliário dos EUA** (HMDA, 2018–2025): quem consegue financiar a casa, onde ela ficou mais cara em relação à renda, o que a alta de juros de 2022 fez e **quanto o dado do próprio governo muda** entre as três versões que ele publica de cada ano. Todo número é conferido contra os totais oficiais antes de poder ser publicado.

🇺🇸 [Read in English](README.md)

## 🔗 Acessar o projeto

[![Abrir o projeto](https://img.shields.io/badge/%E2%96%B6%20Abrir%20o%20projeto-EA580C?style=for-the-badge)](https://diegosantiago1.github.io/us-mortgage-lakehouse/)
[![Ver no portfólio](https://img.shields.io/badge/Ver%20no%20portf%C3%B3lio-1F2937?style=for-the-badge&logo=googlechrome&logoColor=white)](https://diegosantiago1.github.io/Portifolio/#projetos)

**Link direto:** https://diegosantiago1.github.io/us-mortgage-lakehouse/ (abre a página de resultados: mapa dos EUA, juros, negativas, equidade e o capítulo das revisões, em português ou inglês, claro ou escuro).

<!-- RESULTADOS -->

## O problema

Todo ano, cerca de 5 mil instituições financeiras dos EUA são obrigadas por lei (Home Mortgage Disclosure Act) a informar **cada pedido de financiamento imobiliário** que receberam: valor, renda, imóvel, localização, resultado, motivos de negativa, juros e dados demográficos. O governo publica isso linha a linha: só de 2021 são 26 milhões de registros.

O dado tem dois problemas:

1. **Negócio:** onde a casa ficou mais cara em relação à renda? O que a alta dos juros de 2022 fez com a compra e com o refinanciamento? Quem tem o crédito negado e por quê? Perfis parecidos recebem respostas parecidas?
2. **Engenharia:** o mesmo ano é publicado **três vezes** (Snapshot, One Year e Three Year), porque os bancos reenviam e corrigem dados. A versão final só sai quase três anos depois, e o arquivo público **não tem identificador do empréstimo** (removido por privacidade). Por isso não dá para comparar versões com um `MERGE` por chave.

Trabalho numa concessionária em Recife, onde vejo financiamento de carro ser aprovado e recusado todo dia. Quis entender o mesmo problema em escala nacional, com o maior dado público de crédito que existe.

## Arquitetura

```
Arquivos do FFIEC (12 zips, 12 GB) ──► manifesto (URL, ETag, SHA-256) ──► C:\dados\hmda   (bruto, intocado)
                                                                                │
BRONZE   tudo texto + metadados da carga, partição (ano, versão)         ◄──────┘   222,7 mi linhas, 0 malformadas
   │        idempotente (SHA-256), replaceWhere por partição, RESTORE se a contagem não bate
   ├──► REVISÕES   diferença de multiconjuntos pelo hash da linha (96 bits), por banco e por campo
   ▼
SILVER   a versão vigente de cada ano, tipada; NA/Exempt -> nulo (nunca zero); 9 CHECK constraints
   │        versões aplicadas na ordem de publicação do governo -> cada uma é um commit Delta (time travel)
   ▼
QUALIDADE  bloqueante: estado × resultado = API do FFIEC, registros por banco = Transmittal Sheet, linhas conservadas
   ▼
GOLD     preço/renda, mercado e juros, negativas e motivos, equidade (observado ÷ esperado), impacto das revisões
   ▼
PÁGINA   JSON agregado -> site estático (D3, mapa dos EUA, PT/EN) no GitHub Pages
```

Tudo roda localmente em Docker (Python 3.13, Java 21, PySpark 4.2.0 e delta-spark 4.4.1, com os jars embutidos na imagem, então roda com `--network none`). Há um comando por etapa e nenhum orquestrador: o pipeline roda poucas vezes por ano, quando o governo publica uma versão nova.

## Destaques de engenharia

| O quê | Como |
|---|---|
| Ingestão idempotente | Arquivo já carregado (mesmo SHA-256) é ignorado; arquivo republicado substitui só a sua partição (`replaceWhere`). O download retoma com HTTP Range. |
| Nada se perde em silêncio | As linhas são contadas ao extrair o zip, sem o Spark. Se a tabela Delta tem outro número, ela volta à versão anterior com `RESTORE`. Entraram 222.705.887 linhas, foram gravadas 222.705.887, 0 malformadas. |
| Mudanças sem chave | Cada linha vira uma impressão digital de 96 bits (xxhash64 + murmur3 do texto das 99 colunas, com marca de nulo). As versões são comparadas como **multiconjuntos**, porque linhas idênticas repetidas são legítimas. Toda execução é conferida contra o `exceptAll` exato do Spark num estado inteiro. |
| "Qual campo foi corrigido?" | Um hash por coluna, combinado por XOR: "a linha sem a coluna X" = total XOR hash(X). A primeira versão (99 hashes de 98 colunas) estourou a memória da JVM num teste de 3 linhas. |
| Qualidade bloqueante | Estado × resultado contra a API do Data Browser do FFIEC, célula por célula; registros por banco contra o Transmittal Sheet (~4.400 bancos). Se algo falha, a gold e a página se recusam a rodar. |
| Time travel com significado | A silver recebe cada versão do governo na **ordem de publicação** (datas de congelamento lidas no código do site do FFIEC), então `VERSION AS OF` responde "o que um analista via em maio de 2023?". |
| CHECK constraints | 9 restrições de domínio na tabela Delta; um teste prova que elas barram até escrita direta que não passa pelo pipeline. |

## O que o dado real me ensinou (bugs achados e corrigidos)

- **303.002 rejeições falsas em 2018.** As primeiras regras rejeitavam linhas que o governo publica e conta:
  - bancos isentos informam o motivo da negativa como `1111`;
  - alguns juros não cabiam num `decimal(8,3)` (taxa de 260.000%);
  - aparece idade `9999` para o solicitante.

  A tabela de rejeitados, que guarda o texto original, mostrou a causa em minutos. Regra nova: número legível, mesmo absurdo, vira **alerta**, nunca rejeição.
- **Silver lenta (1.900 linhas/s por núcleo).** Eu repetia a mesma limpeza dentro de cada regra, e o código gerado cresceu até o Spark cair para avaliação interpretada. Além disso, funções com lambda (`exists`, `array_compact`) sempre rodam interpretadas no Spark. Separar a transformação em estágios (limpar → tipar → validar) e trocar essas funções por `OR` e `concat_ws` levou um ano de 803 s para 548 s.
- **Retomada de download que misturava dois arquivos.** Se a conexão caía e o governo trocava o arquivo antes da nova tentativa, o Range colava o começo antigo no fim novo. Com tamanhos iguais, o zip corrompido passava. Corrigido guardando o ETag junto do arquivo parcial; um teste reproduz o caso.

## Como rodar

```bash
# 1. Imagem (em máquina com antivírus que inspeciona HTTPS, passe o certificado raiz como segredo do build)
docker compose build
# 2. Pipeline (cada etapa é idempotente)
docker compose run --rm spark python -m hmda.baixar --escopo      # 12 arquivos, ~12 GB
docker compose run --rm spark python -m hmda.bronze --escopo      # ~80 min
docker compose run --rm spark python -m hmda.silver --escopo
docker compose run --rm spark python -m hmda.dq                   # sai com 1 se algo falhar
docker compose run --rm spark python -m hmda.revisoes
docker compose run --rm spark python -m hmda.gold
docker compose run --rm spark python -m hmda.exportar_site
# 3. Testes
docker compose run --rm spark pytest
```

## Limitações

- O dado público **não tem score de crédito nem patrimônio**. O capítulo de equidade controla renda, dívida/renda, LTV, valor, tipo de empréstimo e estado, e diz com todas as letras que uma diferença não prova discriminação.
- Sem identificador do empréstimo, uma linha corrigida aparece como "saiu uma, entrou outra". A análise por campo conta pares que diferem em exatamente uma coluna, sem afirmar qual linha virou qual.
- A API oficial só responde pela versão mais recente de cada ano (e só por linhas com estado). As versões antigas são conferidas internamente, não contra o governo.
- O dado é informado pelos bancos. Valores como juros de 260.000% são mantidos e marcados, e as análises os filtram.

## Decisões

Cada decisão, com a medição por trás e a alternativa descartada, está em [docs/DECISOES.md](docs/DECISOES.md). O plano está em [docs/PLAN.md](docs/PLAN.md).

## Dados e licença

Fonte: FFIEC / CFPB, HMDA Modified LAR e Transmittal Sheet (https://ffiec.cfpb.gov/data-publication/). O catálogo oficial não traz licença explícita; o dado é publicado por lei para uso público. Aqui só são publicados agregados.

---

Diego Freitas Santiago · [GitHub](https://github.com/DiegoSantiago1) · [LinkedIn](https://www.linkedin.com/in/diego-freitas-santiago) · [Portfólio](https://diegosantiago1.github.io/Portifolio/)
