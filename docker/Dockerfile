FROM trinodb/trino:latest
LABEL authors="liubodong"

RUN mkdir -p /etc/hadoop/conf && mkdir -p /opt/iceberg-rest-catalog-server/lib
COPY target/main.jar /opt/iceberg-rest-catalog-server/main.jar
COPY target/lib/ /opt/iceberg-rest-catalog-server/lib
WORKDIR /opt/iceberg-rest-catalog-server
ENV HADOOP_CONF_DIR=/etc/hadoop/conf

ENTRYPOINT ["java", "-jar", "main.jar"]