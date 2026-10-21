# Orquestração com Airflow (D44)

O governo (FFIEC/CFPB) publica e republica os arquivos do HMDA **sem aviso**. Antes do Airflow,
alguém precisava lembrar de conferir e rodar os sete comandos na ordem certa. Agora um DAG faz
isso todo mês.

```
dia 1º, 06:00 UTC
vigiar_publicacoes ──(nada mudou: as demais tarefas nem começam)
        │ mudou algo (ou forcar = true)
        ▼
     baixar ──► bronze ──► silver ──► qualidade ──► gold ──┐
                  │                                         ├──► pagina
                  └──────► revisoes ─────────────────────────┘
```

## O vigia (`hmda.vigia`)

Só faz HEAD no servidor do FFIEC, nada é baixado:

1. **Algum arquivo do projeto mudou?** Compara o ETag e o tamanho de cada um dos 12 arquivos com o
   manifesto. ETag diferente (republicação) ou arquivo nunca baixado → o pipeline roda.
2. **Saiu alguma versão que o projeto ainda não usa?** Pergunta pelo próximo ano de cada versão
   (Snapshot 2026, One Year 2025, Three Year 2023). Se responder 200, é um aviso: entrar com um
   ano novo pede mudar `fontes.DISPONIVEIS` e rever o escopo, e isso não deve ser automático.

Se não conseguir conferir (rede, erro 500), a tarefa **falha**: não dá para afirmar que "nada
mudou". Também roda sozinho: `docker compose run --rm spark python -m hmda.vigia`.

## Decisões do DAG (`airflow/dags/hmda_lakehouse.py`)

- **As dependências seguem as tabelas que cada etapa lê:** as revisões leem só a bronze; o gold
  lê só a silver; a página lê o gold, as revisões e o resultado da qualidade.
- **A qualidade bloqueia:** se `hmda.dq` falhar, gold e página não rodam e a página publicada
  continua a de antes.
- **Uma tarefa por vez** (`max_active_tasks=1`): cada etapa abre uma sessão Spark de vários GB.
- **Novas tentativas só no download** (3, com espera crescente a partir de 10 min): é onde a
  falha costuma ser passageira. Nas etapas de Spark, falha é bug ou dado: tentar de novo esconderia.
- **Idempotência:** cada etapa já era idempotente (mesmo hash = nada a fazer), então rodar de
  novo, ou rodar com `forcar = true`, é seguro.

## Por que em cima da imagem do lakehouse

A imagem do Airflow (`airflow/Dockerfile`) parte da imagem do projeto, então as tarefas rodam os
mesmos comandos do README (`python -m hmda.<etapa>`) com o mesmo Java, PySpark e Delta. O Airflow
3.3.2 fica num **ambiente virtual separado** (`/opt/airflow-venv`): o arquivo de restrições dele
fixa versões de dezenas de pacotes e não pode mexer nas bibliotecas do Spark. O agendador roda pelo
venv; as tarefas chamam o Python do sistema.

## Como rodar

```bash
docker compose build spark                                  # a imagem base
docker compose --profile airflow build airflow
docker compose --profile airflow up -d airflow              # interface em http://127.0.0.1:8080
# ou, sem a interface, uma execução de teste do DAG inteiro:
docker compose --profile airflow run --rm airflow sh -c \
  "/opt/airflow-venv/bin/airflow db migrate && /opt/airflow-venv/bin/airflow dags test hmda_lakehouse"
```

Local, com a porta presa ao 127.0.0.1 e sem tela de login. Na nuvem, o Airflow gerenciado da AWS
(MWAA) custaria centenas de dólares por mês para um pipeline que roda uma vez por mês.

## Execução de ponta a ponta (21/10/2026, medida)

`airflow dags test hmda_lakehouse --conf '{"forcar": true}'` sobre o lake real (12 arquivos, ~225 mi
linhas na bronze):

| Tarefa | Tempo | O que fez |
|---|---|---|
| vigiar_publicacoes | 8 s | 12 HEADs, nada mudou; seguiu porque `forcar = true` |
| baixar | 4 s | os 12 zips já estavam com o hash certo: nada baixado |
| bronze | 40 s | as 12 cargas já registradas com o mesmo hash: nada a fazer |
| silver | 35 s | cada ano já com a versão vigente: nada a fazer |
| revisoes | 2 h 10 min | as seis comparações linha a linha (três por ano) refeitas |
| qualidade | 1 min 6 s | as checagens passaram |
| gold | **falhou** em 9 min | time travel da silver recusado pelo Delta (D45) |

A falha mostrou o desenho funcionando: gold e página não rodaram, e a página publicada continuou a
de antes. Depois da correção (retenção declarada na silver), `airflow tasks test` em silver (1 min),
qualidade (1 min 19 s), gold (17 min) e página (1 min 46 s) fechou a cadeia. A página regenerada saiu
**idêntica à publicada**, exceto pelas datas de execução: as revisões de 2021 deram de novo
1.050.143 linhas que entraram e 1.007.527 que saíram entre o One Year e o Three Year.

## Testes

- `tests/test_vigia.py` (12): republicação, arquivo nunca baixado, versão nova fora do projeto,
  erro de rede, erro 500 que não pode ser confundido com "não publicada", a decisão final e um
  servidor HTTP de verdade respondendo 404.
- `airflow/test_dag.py` (4, roda dentro da imagem do Airflow e no CI): tarefas e comandos,
  dependências, agenda e limites, novas tentativas só no download.
- `tests/test_silver.py::test_retencao_longa_para_o_time_travel_do_gold`: a retenção vale numa
  silver nova, é corrigida numa silver antiga mesmo quando não há nada a gravar, e não cria commit
  quando já está certa.
