# Base Flink image
FROM flink:1.17.2-scala_2.12-java11

WORKDIR /opt/flink

# Copy compiled JAR
COPY ./target/all-1.2.2.jar ./jars/

EXPOSE 8081

CMD ["jobmanager"]
