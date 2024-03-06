#!/bin/bash
rm -f ca.* cert.*
rm -fr broker producer kafka-connector 
mkdir -p broker producer kafka-connector 

validity=365
storetype=PKCS12

# Create your own CA key
openssl req -new -newkey rsa:2048 -noenc -x509 -keyout ca.key -out ca.crt -days ${validity} -subj "/C=IT/L=Milan/O=Lightstreamer Srl/CN=www.lightstreamer.com"
cat ca.crt ca.key > ca.pem

for service in broker # producer kafka-connector
do
#     # Add the generated CA to the truststore file
#     keytool -keystore $service/$service.truststore.jks -alias CARoot -importcert -file ca.cert -storepass "$service-truststore-password" -storetype $storetype -noprompt

#     # Create the keystore file
#     keytool -genkey -noprompt -keystore $service/$service.keystore.jks -storetype $storetype -alias $service -keyalg RSA -validity ${validity} -storepass "$service-password" -keypass "$service-password" -dname "CN=$service.example.lightsteramer.com, OU=Kafka Connector, O=Lighsteramer Srl, C=Italy"

#     # Create the CSR file
#     keytool -keystore $service/$service.keystore.jks -alias $service -storetype $storetype -certreq -file $service/$service.csr -storepass "$service-password"

    openssl req -new -newkey rsa:2048 -keyout $service.key -out $service.csr -noenc -passin pass:$service-password
     # Sign the key
    #  openssl x509 -req -days ${validity} \
    #     -in $service/$service.csr \
    #     -CA ca.crt \
    #     -CAkey ca.key \
    #     -CAcreateserial \
    #     -out $service/$service.crt \
    #     -passin pass:$service-password

    # openssl pkcs12 -export \
    #     -in $service/$service.crt \
    #     -inkey $service/$service.key \
    #     -chain \
    #     -CAfile ca.pem \
    #     -name $service \
    #     -out $service/$service.p12
    #     -password pass:$service-password

    # keytool -importkeystore \
    #     -deststorepass $service-password \
    #     -destkeystore $service/$service.keystore.pkcs12 \
    #     -srckeystore $service/$service.p12 \
    #     -deststoretype PKCS12  \
    #     -srcstoretype PKCS12 \
    #     -noprompt \
    #     -srcstorepass $service-password

#     # Import both the certificate of the CA and the signed certificate back into the keystore
#     keytool -keystore $service/$service.keystore.jks -storetype $storetype -alias CARoot -importcert -file ca.cert -storepass "$service-password" -noprompt
#     keytool -keystore $service/$service.keystore.jks -storetype $storetype -alias $service -importcert -file $service/$service.signed -storepass "$service-password" -noprompt
done

# # Save credentials
# echo "broker-password" > broker/broker_keystore_credentials
# echo "broker-password" > broker/broker_key_credentials
# echo "broker-truststore-password" > broker/broker_truststore_credentials