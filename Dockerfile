# syntax=docker/dockerfile:1
# Imagem do lakehouse: Python + Java + PySpark + Delta, versões fixas (D15).
#
# Build normal:
#   docker compose build
# Em máquina com antivírus que inspeciona HTTPS (ex.: Norton), passe o certificado
# raiz como segredo do build; ele NÃO fica em nenhuma camada da imagem:
#   docker build --secret id=ca,src=C:/dados/hmda/certs/norton-ca.pem -t us-mortgage-lakehouse:local .

FROM python:3.13-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    SPARK_LOCAL_IP=127.0.0.1 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Java 21 (o Spark 4 pede 17 ou 21) e o `ps` usado pelos scripts do Spark.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

# Usuário sem privilégios; /lake existe com dono certo para o volume Docker.
RUN useradd --create-home --uid 1000 hmda \
 && mkdir -p /app /dados /lake /opt/delta-jars \
 && chown hmda:hmda /app /dados /lake /opt/delta-jars

COPY requirements.txt /tmp/requirements.txt
RUN --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then \
      cat /etc/ssl/certs/ca-certificates.crt /run/secrets/ca > /tmp/ca.pem; \
      export PIP_CERT=/tmp/ca.pem; \
    fi; \
    pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm -f /tmp/ca.pem /tmp/requirements.txt

USER hmda
WORKDIR /app
COPY --chown=hmda:hmda src/ /app/src/
COPY --chown=hmda:hmda docker/aquecer.py docker/separar_jars.py /tmp/build/

# 1) O delta-spark resolve os jars pelo Maven (cache do Ivy).
# 2) Só os jars que o PySpark não tem vão para /opt/delta-jars.
# 3) O cache do Ivy e os temporários são apagados na MESMA camada.
# Com o segredo, o Java confia no certificado extra só durante este passo.
RUN --mount=type=secret,id=ca,required=false,uid=1000 \
    if [ -f /run/secrets/ca ]; then \
      cp "$JAVA_HOME/lib/security/cacerts" /tmp/build/ts \
      && keytool -importcert -noprompt -keystore /tmp/build/ts -storepass changeit \
           -alias extra -file /run/secrets/ca >/dev/null \
      && export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/tmp/build/ts -Djavax.net.ssl.trustStorePassword=changeit"; \
    fi; \
    python /tmp/build/aquecer.py \
 && python /tmp/build/separar_jars.py "$HOME/.ivy2.5.2/jars" /opt/delta-jars \
 && rm -rf "$HOME/.ivy2.5.2" /tmp/build /tmp/* 2>/dev/null; \
    test -n "$(ls /opt/delta-jars)"

# Daqui em diante, o Spark usa só os jars locais (funciona sem internet).
ENV HMDA_JARS=/opt/delta-jars
RUN python -c "import tempfile; from hmda.spark import criar_sessao; \
s = criar_sessao(driver_memory='1g', nucleos='1'); d = tempfile.mkdtemp(); \
s.range(3).write.format('delta').save(d + '/t'); \
assert s.read.format('delta').load(d + '/t').count() == 3; s.stop()" \
 && (rm -rf /tmp/* 2>/dev/null || true)

COPY --chown=hmda:hmda tests/ /app/tests/
COPY --chown=hmda:hmda pyproject.toml /app/

CMD ["python", "-c", "print('use: docker compose run --rm spark python -m hmda.<etapa>')"]
