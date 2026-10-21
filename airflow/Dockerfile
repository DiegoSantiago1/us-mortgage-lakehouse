# syntax=docker/dockerfile:1
# Airflow em cima da imagem do lakehouse (D44): as tarefas rodam os mesmos comandos de
# sempre (python -m hmda.<etapa>), com o mesmo Python, Java, PySpark e Delta.
#
# O Airflow fica num ambiente virtual próprio (/opt/airflow-venv): o arquivo de restrições
# dele fixa versões de dezenas de pacotes e não pode mexer nas bibliotecas do Spark. O
# agendador roda pelo venv; as tarefas chamam o `python` do sistema.
#
#   docker compose build spark                       # a imagem base, se ainda não existe
#   docker compose --profile airflow build airflow
# Com antivírus que inspeciona HTTPS (ex.: Norton), passe o certificado como segredo:
#   docker build --secret id=ca,src=C:/dados/hmda/certs/norton-ca.pem -f airflow/Dockerfile \
#     -t us-mortgage-lakehouse-airflow:local .

ARG BASE=us-mortgage-lakehouse:local
FROM ${BASE}

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.13

USER root
RUN --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then \
      cat /etc/ssl/certs/ca-certificates.crt /run/secrets/ca > /tmp/ca.pem; \
      export PIP_CERT=/tmp/ca.pem CURL_CA_BUNDLE=/tmp/ca.pem; \
    fi; \
    python -m venv /opt/airflow-venv \
 && /opt/airflow-venv/bin/pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
 && /opt/airflow-venv/bin/pip install --no-cache-dir pytest \
 && rm -f /tmp/ca.pem \
 && mkdir -p /opt/airflow/dags \
 && chown -R hmda:hmda /opt/airflow

USER hmda
# Estado do Airflow (banco SQLite, logs) num volume próprio: sobrevive ao container.
RUN mkdir -p /opt/airflow/estado
# O venv vai no FIM do PATH: o `airflow standalone` chama os processos filhos pelo nome
# `airflow`, e o `python` das tarefas continua sendo o do sistema (com PySpark).
ENV PATH="${PATH}:/opt/airflow-venv/bin" \
    AIRFLOW_HOME=/opt/airflow/estado \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DEFAULT_TIMEZONE=UTC \
    AIRFLOW__API__PORT=8080

COPY --chown=hmda:hmda airflow/dags/ /opt/airflow/dags/
COPY --chown=hmda:hmda airflow/test_dag.py /opt/airflow/test_dag.py

EXPOSE 8080
CMD ["/opt/airflow-venv/bin/airflow", "standalone"]
