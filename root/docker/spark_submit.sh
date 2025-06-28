#!/bin/bash
# Script to submit Spark application with proper security settings

# Set environment variables to disable Kerberos authentication
export HADOOP_USER_NAME=root
export HADOOP_CONF_DIR=/opt/bitnami/spark/conf
export HADOOP_HOME=/opt/bitnami/spark
export JAVA_HOME=/opt/bitnami/java

# Create necessary directories with proper permissions
mkdir -p /tmp/.sparkStaging /tmp/spark-events /tmp/spark_checkpoints /tmp/.ivy2
chmod -R 777 /tmp/.sparkStaging /tmp/spark-events /tmp/spark_checkpoints /tmp/.ivy2

# Set system properties to disable Kerberos
export SPARK_SUBMIT_OPTS="
-Dlog4j2.configurationFile=log4j2.xml 
-Dhadoop.security.authentication=simple 
-Djavax.security.auth.useSubjectCredsOnly=false 
-Djava.security.krb5.conf=/dev/null 
--add-opens=java.base/java.nio=ALL-UNNAMED
"

# Submit the Spark application
exec /opt/bitnami/spark/bin/spark-submit \
  --class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-java-options "$SPARK_SUBMIT_OPTS" \
  --executor-memory 1g \
  --driver-memory 1g \
  --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED" \
  --conf "spark.hadoop.fs.defaultFS=file:///" \
  --conf "spark.hadoop.hadoop.security.authentication=simple" \
  --conf "spark.kerberos.keytab=none" \
  --conf "spark.kerberos.principal=none" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryo.registrator=com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator" \
  --conf "spark.kryoserializer.buffer.max=512m" \
  --conf "spark.kryoserializer.buffer=256k" \
  --conf "spark.kryo.registrationRequired=false" \
  --conf "spark.kryo.unsafe=true" \
  --conf "spark.serializer.objectStreamReset=100" \
  --conf "spark.serializer.javaSerializerReuse=true" \
  --conf "spark.serializer.useJavaSerializer=true" \
  --conf "spark.default.parallelism=1" \
  --conf "spark.sql.shuffle.partitions=1" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.3 \
  "$@" 