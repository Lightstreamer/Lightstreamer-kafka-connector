#!/bin/bash
rm -f ca.cert ca.key cert.file cert.signed
rm -fr broker kafka-connector client
mkdir -p broker kafka-connector client

validity=365
host=broker
# Generate the keys and the certificates
keytool -keystore broker/kafka.broker.keystore.pkcs12 -alias localhost -genkey -keyalg RSA -validity ${validity} -storepass password -keypass password -dname "CN=broker.example.lightsteramer.com, OU=Kafka Connector, O=Lighsteramer Srl, C=Italy" -ext SAN=DNS:${host}

# Create your own CA
openssl req -new -newkey rsa:2048 -noenc -x509 -keyout ca.key -out ca.cert -days ${validity} -subj "/C=IT/L=Milan/O=Lightstreamer Srl/CN=www.lightstreamer.com"

# Add the generated CA to the connector's truststore and broker's truststore
keytool -keystore kafka-connector/kafka.connector.truststore.pkcs12 -alias CARoot -importcert -file ca.cert -storepass password -noprompt
keytool -keystore client/kafka.client.truststore.pkcs12 -alias CARoot -importcert -file ca.cert -storepass password -noprompt
keytool -keystore broker/kafka.broker.truststore.pkcs12 -alias CARoot -importcert -file ca.cert -storepass password -noprompt

# Sign the broker's certificates with the generated CA.
# Export the certificate from the broker's keystore
keytool -keystore broker/kafka.broker.keystore.pkcs12 -alias localhost -certreq -file cert.file -storepass password
# Sign it with the CA
openssl x509 -req -CA ca.cert -CAkey ca.key -in cert.file -out cert.signed -days ${validity} -CAcreateserial -passin pass:password
# Import both the certificate of the CA and the signed certificate into the broker keystore:
keytool -keystore broker/kafka.broker.keystore.pkcs12 -alias CARoot -importcert -file ca.cert -storepass password -noprompt
keytool -keystore broker/kafka.broker.keystore.pkcs12 -alias localhost -importcert -file cert.signed -storepass password -noprompt

# Save credentials
echo "password" > broker/broker_keystore_credentials
echo "password" > broker/broker_key_credentials