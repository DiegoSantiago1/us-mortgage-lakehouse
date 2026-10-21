"""Estrutura do DAG (D44). Roda dentro da imagem do Airflow:

    docker compose --profile airflow run --rm airflow
        /opt/airflow-venv/bin/python -m pytest -q /opt/airflow/test_dag.py

(uma linha só no terminal).
"""

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

try:  # Airflow 3.1+
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # pragma: no cover
    from airflow.models.dagbag import DagBag

PASTA_DAGS = Path(__file__).resolve().parent / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(PASTA_DAGS))  # exemplos desligados: AIRFLOW__CORE__LOAD_EXAMPLES
    assert bag.import_errors == {}, bag.import_errors
    return bag.dags["hmda_lakehouse"]  # o que foi lido do arquivo, sem consultar o banco


def _acima(dag, tarefa: str) -> set[str]:
    return set(dag.get_task(tarefa).upstream_task_ids)


def test_tarefas_e_comandos(dag) -> None:
    comandos = {t.task_id: getattr(t, "bash_command", None) for t in dag.tasks}
    assert comandos == {
        "vigiar_publicacoes": None,
        "baixar": "python -m hmda.baixar --escopo",
        "bronze": "python -m hmda.bronze --escopo",
        "silver": "python -m hmda.silver --escopo",
        "qualidade": "python -m hmda.dq",
        "revisoes": "python -m hmda.revisoes",
        "gold": "python -m hmda.gold",
        "pagina": "python -m hmda.exportar_site",
    }


def test_ordem_segue_as_tabelas_que_cada_etapa_le(dag) -> None:
    assert _acima(dag, "baixar") == {"vigiar_publicacoes"}
    assert _acima(dag, "bronze") == {"baixar"}
    assert _acima(dag, "silver") == {"bronze"}
    assert _acima(dag, "revisoes") == {"bronze"}
    assert _acima(dag, "qualidade") == {"silver"}
    assert _acima(dag, "gold") == {"qualidade"}  # qualidade falhando bloqueia o gold
    assert _acima(dag, "pagina") == {"gold", "revisoes"}


def test_agenda_e_limites(dag) -> None:
    assert dag.schedule == "0 6 1 * *" or str(getattr(dag, "timetable", "")).find("0 6 1 * *") >= 0
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 1  # uma sessão Spark por vez
    assert dag.params["forcar"] is False


def test_novas_tentativas_so_no_download(dag) -> None:
    assert dag.get_task("baixar").retries == 3
    assert dag.get_task("baixar").retry_delay == timedelta(minutes=10)
    for tarefa in ("bronze", "silver", "qualidade", "revisoes", "gold", "pagina"):
        assert dag.get_task(tarefa).retries == 0, tarefa
