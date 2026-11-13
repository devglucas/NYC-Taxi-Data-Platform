FROM apache/airflow:3.4.0-python3.14

# Java para o Spark local dentro das tarefas. A imagem base (Debian endurecida) só traz o
# pacote de certificados em PEM, e o script do ca-certificates-java não roda nela; sem um
# trust store, o Java não baixa os jars do Delta. Ele é montado aqui a partir do pacote PEM.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/ssl/certs/java /tmp/ca \
    && csplit -s -z -f /tmp/ca/ca- /etc/ssl/certs/ca-certificates.crt '/-----BEGIN CERTIFICATE-----/' '{*}' \
    && for f in /tmp/ca/ca-*; do \
         keytool -importcert -noprompt -storetype jks -keystore /etc/ssl/certs/java/cacerts \
           -storepass changeit -alias "$(basename "$f")" -file "$f" > /dev/null; \
       done \
    && rm -rf /tmp/ca
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir pyspark==4.2.0 delta-spark==4.4.1 duckdb==2.0.0 matplotlib==3.11.2 pytest==9.1.1

# dbt em um ambiente próprio, para não disputar dependências com o Airflow.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-duckdb==1.11.1 duckdb==2.0.0
ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt

# Baixa uma vez, na construção, os jars do Delta e a extensão delta do DuckDB.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; \
configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()" \
    && python -c "import duckdb; duckdb.sql('install delta')"
