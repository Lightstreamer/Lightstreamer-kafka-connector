#!/bin/bash
rm -f ca.* cert.*
services="broker producer kafka-connector schema-registry"

rm -fr $services
mkdir -p $services

storetype="-storetype JKS"
keypass_suffix="private-key-password"
validity=365

# Create your own CA key
openssl req -new -newkey rsa:2048 -noenc -x509 -keyout ca.key -out ca.cert -days ${validity} -subj "/C=IT/L=Milan/O=Lightstreamer Srl/CN=www.lightstreamer.com"

for service in $services
do
    # Add the generated CA to the trust store file
    keytool -keystore $service/$service.truststore.jks -alias CARoot -importcert -file ca.cert -storepass "$service-truststore-password" -noprompt

    # Create the key store file
    keypass="-keypass $service-${keypass_suffix}"
    keytool -genkey -noprompt $storetype $keypass -keystore $service/$service.keystore.jks -alias $service -keyalg RSA -validity ${validity} -storepass "$service-password" -dname "CN=$service.example.lightsteramer.com, OU=Kafka Connector, O=Lighsteramer Srl, C=Italy"

    # Create the CSR file
    keytool -keystore $service/$service.keystore.jks $storetype $keypass -alias $service -certreq -file $service/$service.csr -storepass "$service-password"

    # Sign the key
    openssl x509 -req -CA ca.cert -CAkey ca.key -in $service/$service.csr -out $service/$service.signed -days ${validity} -CAcreateserial -passin pass:$service-password

    # Import both the certificate of the CA and the signed certificate back into the key store
    keytool -keystore $service/$service.keystore.jks -alias CARoot -importcert -file ca.cert -storepass "$service-password" -noprompt
    keytool -keystore $service/$service.keystore.jks $keypass -alias $service -importcert -file $service/$service.signed -storepass "$service-password" -noprompt
done

# Save credentials
echo "broker-password" > broker/broker_keystore_credentials
echo "broker-${keypass_suffix}" > broker/broker_key_credentials
echo "broker-truststore-password" > broker/broker_truststore_credentials