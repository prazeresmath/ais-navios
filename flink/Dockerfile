# Flink 1.20 (LTS) + PyFlink + conector Kafka.
# A imagem oficial é Ubuntu 22.04 (Python 3.10), compatível com o PyFlink 1.20.
FROM flink:1.20.5-scala_2.12-java17

ARG KAFKA_CONNECTOR=3.3.0-1.20

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Conector Kafka (KafkaSource / KafkaSink) no classpath do Flink
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar

# Pasta de checkpoints: criada aqui para o volume nascer com o dono certo (usuário flink)
RUN mkdir -p /flink-data/checkpoints && chown -R flink:flink /flink-data

COPY --chown=flink:flink job /opt/job
COPY --chown=flink:flink submit.sh /opt/submit.sh
RUN sed -i "s/\r$//" /opt/submit.sh && chmod +x /opt/submit.sh
