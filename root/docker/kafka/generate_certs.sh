#!/bin/bash
# Generates self-signed certificates for securing Kafka communication.

set -e

# Define directories
CERTS_DIR="kafka_certs"
CA_DIR="$CERTS_DIR/ca"
BROKER_DIR="$CERTS_DIR/broker"

# Clean up previous certificates
rm -rf $CERTS_DIR
mkdir -p $CA_DIR $BROKER_DIR

# --- Certificate Authority (CA) ---
echo "Generating CA..."
openssl req -new -x509 -keyout $CA_DIR/ca.key -out $CA_DIR/ca.crt -days 365 -nodes -subj "/C=US/ST=CA/L=PaloAlto/O=TabulaRasa/CN=KafkaCA"

# --- Truststore for the CA ---
echo "Creating CA truststore..."
keytool -genkey -keystore $BROKER_DIR/kafka.truststore.jks -alias CARoot -validity 365 -storepass 'password' -keypass 'password' -dname "CN=KafkaCA" -storetype pkcs12
keytool -keystore $BROKER_DIR/kafka.truststore.jks -alias CARoot -import -file $CA_DIR/ca.crt -storepass 'password' -noprompt

# --- Keystore for Kafka Broker ---
echo "Generating broker keystore..."
keytool -genkey -keystore $BROKER_DIR/kafka.keystore.jks -alias kafka-broker -validity 365 -storepass 'password' -keypass 'password' -dname "CN=kafka" -storetype pkcs12

# --- Create Certificate Signing Request (CSR) for the Broker ---
keytool -keystore $BROKER_DIR/kafka.keystore.jks -alias kafka-broker -certreq -file $BROKER_DIR/broker.csr -storepass 'password'

# --- Sign the Broker's CSR with the CA ---
openssl x509 -req -in $BROKER_DIR/broker.csr -CA $CA_DIR/ca.crt -CAkey $CA_DIR/ca.key -CAcreateserial -out $BROKER_DIR/broker.crt -days 365

# --- Import CA certificate and signed broker certificate into the broker's keystore ---
keytool -keystore $BROKER_DIR/kafka.keystore.jks -alias CARoot -import -file $CA_DIR/ca.crt -storepass 'password' -noprompt
keytool -keystore $BROKER_DIR/kafka.keystore.jks -alias kafka-broker -import -file $BROKER_DIR/broker.crt -storepass 'password'

echo "Certificates generated successfully in $CERTS_DIR."
echo "Use 'password' as the keystore and truststore password." 