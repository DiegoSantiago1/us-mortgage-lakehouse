"""
### Lakehouse do HMDA: do arquivo do governo à página (D44)

Todo dia 1º às 06:00 UTC, o **vigia** pergunta ao FFIEC (só HEAD, nada é baixado) se algum
arquivo que o projeto usa foi republicado e se saiu alguma versão nova. Se nada mudou, as
demais tarefas nem começam (*short-circuit*). Se mudou, roda a cadeia na ordem:

`baixar → bronze → silver → qualidade → gold → página`, e `bronze → revisões → página`
(as revisões leem só a bronze; a página lê o gold, as revisões e o resultado da qualidade).

- **Qualidade bloqueia:** se `hmda.dq` falhar, gold e página não rodam (a página de antes fica).
- **Uma tarefa por vez** (`max_active_tasks=1`): cada etapa abre uma sessão Spark de vários GB.
- **Novas tentativas** só onde a falha costuma ser passageira (rede, no download).
- Cada etapa é idempotente (mesmo hash = nada a fazer), então rodar de novo é seguro.

Para rodar mesmo sem mudança (ex.: depois de alterar o código), dispare com `forcar = true`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, get_current_context, task

PASTA_APP = "/app"


def etapa(task_id: str, comando: str, **opcoes: object) -> BashOperator:
    """Uma etapa do pipeline: o mesmo `python -m hmda.<etapa>` do README, no Python do sistema."""
    return BashOperator(task_id=task_id, bash_command=comando, cwd=PASTA_APP, **opcoes)


@dag(
    dag_id="hmda_lakehouse",
    description="Confere publicações do FFIEC; se mudou algo, roda a cadeia até a página",
    schedule="0 6 1 * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 0, "execution_timeout": timedelta(hours=4)},
    params={
        "forcar": Param(
            False,
            type="boolean",
            description="Roda a cadeia inteira mesmo que o vigia não ache nada novo.",
        )
    },
    tags=["hmda", "lakehouse", "spark"],
    doc_md=__doc__,
)
def hmda_lakehouse() -> None:
    @task.short_circuit(task_id="vigiar_publicacoes")
    def vigiar_publicacoes() -> bool:
        from hmda import vigia  # só biblioteca padrão: roda no Python do Airflow

        relatorio = vigia.conferir_agora()
        print(relatorio.resumo())
        # Erro de rede falha a tarefa (não dá para afirmar que "nada mudou").
        return vigia.decidir(relatorio, forcar=bool(get_current_context()["params"]["forcar"]))

    baixar = etapa(
        "baixar",
        "python -m hmda.baixar --escopo",
        retries=3,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
    )
    bronze = etapa("bronze", "python -m hmda.bronze --escopo")
    silver = etapa("silver", "python -m hmda.silver --escopo")
    qualidade = etapa("qualidade", "python -m hmda.dq")
    revisoes = etapa("revisoes", "python -m hmda.revisoes")
    gold = etapa("gold", "python -m hmda.gold")
    pagina = etapa("pagina", "python -m hmda.exportar_site")

    vigiar_publicacoes() >> baixar >> bronze
    bronze >> silver >> qualidade >> gold
    bronze >> revisoes
    [gold, revisoes] >> pagina


hmda_lakehouse()
