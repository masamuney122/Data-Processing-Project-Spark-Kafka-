FROM confluentinc/cp-kafka-connect:7.5.2

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:latest

RUN mkdir -p /etc/hadoop/conf && chmod -R 777 /etc/hadoop

USER appuser
