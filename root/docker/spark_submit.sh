#!/bin/bash
# Script to submit Spark application with proper security settings

# Set environment variables to avoid Kerberos authentication
export HADOOP_USER_NAME=spark
export HADOOP_CONF_DIR=/opt/bitnami/hadoop/etc/hadoop
export HADOOP_HOME=/opt/bitnami/hadoop
export JAVA_HOME=/opt/bitnami/java

# Create necessary directories with appropriate permissions
mkdir -p /tmp/spark-events
chmod 777 /tmp/spark-events
mkdir -p /tmp/spark_checkpoints
chmod 777 /tmp/spark_checkpoints

# Set system properties to disable Kerberos
SPARK_SUBMIT_OPTS="
  -Dlog4j2.configurationFile=log4j2.xml 
  -Dhadoop.security.authentication=simple 
  -Djavax.security.auth.useSubjectCredsOnly=false 
  -Djava.security.krb5.conf=/dev/null 
  -Dsun.io.serialization.extendedDebugInfo=true
  -Dsun.io.serialization.extendedDebugInfo.trace=true
  -Djava.io.serialization.strict=false
  -Djdk.serialFilter=allow
  -Dsun.serialization.validateClassSerialVersionUID=false
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
"

# Submit the Spark application
exec /opt/bitnami/spark/bin/spark-submit \
  --class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-java-options "$SPARK_SUBMIT_OPTS" \
  --executor-memory 1g \
  --driver-memory 1g \
  --conf "spark.executor.extraJavaOptions=$SPARK_SUBMIT_OPTS" \
  --conf "spark.hadoop.fs.defaultFS=file:///" \
  --conf "spark.hadoop.hadoop.security.authentication=simple" \
  --conf "spark.kerberos.keytab=none" \
  --conf "spark.kerberos.principal=none" \
  --conf "spark.serializer=org.apache.spark.serializer.JavaSerializer" \
  --conf "spark.serializer.objectStreamReset=100" \
  --conf "spark.serializer.javaSerializerReuse=true" \
  --conf "spark.serializer.useJavaSerializer=true" \
  --conf "spark.default.parallelism=1" \
  --conf "spark.sql.shuffle.partitions=1" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.7 \
  "$@" 