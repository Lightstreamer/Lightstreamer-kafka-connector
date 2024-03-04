#!/bin/bash
rm -f ca.* cert.*
rm -fr broker producer kafka-connector 
mkdir -p broker producer kafka-connector 

validity=365
host=broker

# Create your own CA key
openssl req -new -newkey rsa:2048 -noenc -x509 -keyout ca.key -out ca.cert -days ${validity} -subj "/C=IT/L=Milan/O=Lightstreamer Srl/CN=www.lightstreamer.com"

for service in broker producer kafka-connector
do
    # Add the generated CA to the truststore file
    keytool -keystore $service/$service.truststore.jks -alias CARoot -importcert -file ca.cert -storepass "$service-truststore-password" -storetype jks -noprompt

    # Create the keystore file
    keytool -genkey -noprompt -keystore $service/$service.keystore.jks -storetype jks -alias $service -keyalg RSA -validity ${validity} -storepass "$service-password" -keypass "$service-password" -dname "CN=$service.example.lightsteramer.com, OU=Kafka Connector, O=Lighsteramer Srl, C=Italy"

    # Create the CSR file
    keytool -keystore $service/$service.keystore.jks -alias $service -storetype jks -certreq -file $service/$service.csr -storepass "$service-password"

    # Sign the key
    openssl x509 -req -CA ca.cert -CAkey ca.key -in $service/$service.csr -out $service/$service.signed -days ${validity} -CAcreateserial -passin pass:$service-password

    # Import both the certificate of the CA and the signed certificate back into the keystore
    keytool -keystore $service/$service.keystore.jks -storetype jks -alias CARoot -importcert -file ca.cert -storepass "$service-password" -noprompt
    keytool -keystore $service/$service.keystore.jks -storetype jks -alias $service -importcert -file $service/$service.signed -storepass "$service-password" -noprompt
done

# Save credentials
echo "broker-password" > broker/broker_keystore_credentials
echo "broker-password" > broker/broker_key_credentials
echo "broker-truststore-password" > broker/broker_truststore_credentials