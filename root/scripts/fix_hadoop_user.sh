#!/bin/bash
# Fix for KerberosAuthException by setting HADOOP_USER_NAME in containers
echo "Setting HADOOP_USER_NAME=root and creating home directories in Spark containers..."

# Fix for master
docker exec spark-master bash -c "
export HADOOP_USER_NAME=root
export USER_HOME=/tmp
export HOME=/tmp
mkdir -p /tmp/.sparkStaging
chmod 777 /tmp/.sparkStaging
echo 'export HADOOP_USER_NAME=root' >> /opt/bitnami/spark/conf/spark-env.sh
echo 'export USER_HOME=/tmp' >> /opt/bitnami/spark/conf/spark-env.sh
echo 'export HOME=/tmp' >> /opt/bitnami/spark/conf/spark-env.sh
"

# Fix for worker
docker exec spark-worker bash -c "
export HADOOP_USER_NAME=root
export USER_HOME=/tmp
export HOME=/tmp
mkdir -p /tmp/.sparkStaging
chmod 777 /tmp/.sparkStaging
echo 'export HADOOP_USER_NAME=root' >> /opt/bitnami/spark/conf/spark-env.sh
echo 'export USER_HOME=/tmp' >> /opt/bitnami/spark/conf/spark-env.sh
echo 'export HOME=/tmp' >> /opt/bitnami/spark/conf/spark-env.sh
"

echo "Done. Environment variables set and directories created."
