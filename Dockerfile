FROM maven:3.5.4-jdk-7-alpine AS build

COPY . /src
RUN cd /src && mvn clean && mvn -DskipTests package

FROM cloudera/quickstart:latest

COPY --from=build /src/target/spark_kaggle_tempereture_data-1.0-SNAPSHOT.jar /app/app.jar
COPY /data /data

ENTRYPOINT service zookeeper-server start && \
     service hadoop-hdfs-datanode start && service hadoop-hdfs-journalnode start && service hadoop-hdfs-namenode start && \
     service hadoop-hdfs-secondarynamenode start && service hadoop-httpfs start && service hadoop-mapreduce-historyserver start && \
     service hadoop-yarn-nodemanager start && service hadoop-yarn-resourcemanager start && service hbase-master start && service hbase-rest start && service hbase-thrift start && \
     service hive-metastore start && \
     service hive-server2 start && \
     hadoop fs -mkdir /data && \
     hadoop fs -put /data/GlobalTemperatures.csv /data/GlobalTemperatures.csv && \
     hadoop fs -put /data/GlobalLandTemperaturesByCity.csv /data/GlobalLandTemperaturesByCity.csv && \
     hadoop fs -put /data/GlobalLandTemperaturesByCountry.csv /data/GlobalLandTemperaturesByCountry.csv && \
     spark-submit --class Main --master yarn --deploy-mode client /app/app.jar
