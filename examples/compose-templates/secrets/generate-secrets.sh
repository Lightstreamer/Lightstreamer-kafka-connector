#!/bin/bash
#
# Generates the key stores and trust stores used by the examples (demo secrets only: never reuse
# them outside of these examples).
#
# Usage:
#   ./generate-secrets.sh            # reuse the existing CA, if any, and regenerate the key stores
#                                    # and trust stores of all the services
#   ./generate-secrets.sh --new-ca   # also create a new CA, which requires replacing the trust
#                                    # stores everywhere
#   ./generate-secrets.sh --check    # change nothing: print the expiry of the CA and of the
#                                    # service certificates, and fail if any of them expires
#                                    # within CHECK_DAYS days (default: 90)
#
set -euo pipefail

# Work in the folder of this script, whatever the directory it is launched from.
cd "$(dirname "${BASH_SOURCE[0]}")"

services=(broker producer kafka-connector schema-registry)

for tool in keytool openssl; do
    command -v "$tool" >/dev/null || { echo "$tool not found in the PATH" >&2; exit 1; }
done

mode=generate
new_ca=false
if (( $# > 1 )); then
    echo "Usage: $0 [--new-ca | --check]" >&2
    exit 1
fi
case "${1:-}" in
    "") ;;
    --new-ca) new_ca=true ;;
    --check) mode=check ;;
    *) echo "Usage: $0 [--new-ca | --check]" >&2; exit 1 ;;
esac

if [[ $mode == check ]]; then
    check_days=${CHECK_DAYS:-90}
    check_seconds=$((check_days * 86400))
    failed=false

    # Prints the expiry of a certificate given in PEM format, and records a failure if the
    # certificate is unreadable or expires within the threshold.
    report() {
        local label=$1 pem=$2 expiry status="OK"
        if ! expiry=$(openssl x509 -noout -enddate <<< "$pem" 2> /dev/null); then
            printf '%-18s %s\n' "$label" "UNREADABLE"
            failed=true
            return
        fi
        if ! openssl x509 -noout -checkend "$check_seconds" <<< "$pem" > /dev/null; then
            status="EXPIRES WITHIN $check_days DAYS"
            failed=true
        fi
        printf '%-18s %s  %s\n' "$label" "${expiry#notAfter=}" "$status"
    }

    if [[ -f ca.cert ]]; then
        report "ca" "$(cat ca.cert)"
    else
        printf '%-18s %s\n' "ca" "NOT FOUND"
        failed=true
    fi
    for service in "${services[@]}"; do
        keystore=$service/$service.keystore.p12
        if [[ ! -f $keystore ]]; then
            printf '%-18s %s\n' "$service" "NOT FOUND"
            failed=true
            continue
        fi
        pem=$(openssl pkcs12 -in "$keystore" -passin "pass:$service-keystore-password" -nokeys -clcerts 2> /dev/null || true)
        report "$service" "$pem"
    done

    [[ $failed == false ]]
    exit
fi

storetype=(-storetype PKCS12)
# The same RSA key size for the CA and the services, independently of the JDK default.
key_size=3072
# The validity of the certificates of the services, in days. The CA must outlive them.
validity=1825
ca_validity=7300

script_dir=$(pwd)

# Everything is built in a staging folder and moved in place only when all the steps succeed, so
# that a failure never damages the existing secrets.
stage=$(mktemp -d "$script_dir/.staging.XXXXXX")
trap 'rm -rf "$stage"' EXIT

# Create your own CA key, unless it already exists. Keeping the CA allows rotating the service
# certificates without changing the trust stores.
if [[ $new_ca == true || ! -f ca.key || ! -f ca.cert ]]; then
    # The CA is described by a configuration file, which works with every OpenSSL version (the
    # -addext and -noenc options are not available everywhere). The -nodes option creates an
    # unencrypted key.
    cat > "$stage/ca-request.cnf" <<EOF
[req]
distinguished_name = dn
x509_extensions = ca_extensions
prompt = no

[dn]
C = IT
L = Milan
O = Lightstreamer Srl
CN = www.lightstreamer.com

[ca_extensions]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
EOF
    openssl req -new -newkey "rsa:$key_size" -nodes -x509 -config "$stage/ca-request.cnf" \
        -keyout "$stage/ca.key" -out "$stage/ca.cert" -days "$ca_validity"
else
    cp ca.key ca.cert "$stage/"
    if [[ -f ca.srl ]]; then
        cp ca.srl "$stage/"
    fi
fi

cd "$stage"

# Returns the largest number of days, up to the given maximum, for which a certificate signed now
# ends at least one day before the CA certificate does. Binary search on openssl's -checkend, which
# succeeds if the certificate does not expire within the given number of seconds.
max_signable_days() {
    local low=0 high=$1 mid
    while (( low < high )); do
        mid=$(( (low + high + 1) / 2 ))
        if openssl x509 -in ca.cert -noout -checkend $(( (mid + 1) * 86400 )) > /dev/null; then
            low=$mid
        else
            high=$(( mid - 1 ))
        fi
    done
    echo "$low"
}

max_validity=$(max_signable_days "$validity")
if (( max_validity < 1 )); then
    echo "The CA certificate expires too soon to sign new certificates: use --new-ca" >&2
    exit 1
fi
if (( max_validity < validity )); then
    echo "The CA certificate expires earlier: the service certificates will be valid for $max_validity days"
    validity=$max_validity
fi

mkdir -p "${services[@]}"

for service in "${services[@]}"
do
    keystore=$service/$service.keystore.p12
    truststore=$service/$service.truststore.p12

    # Add the generated CA to the trust store file
    keytool -keystore "$truststore" "${storetype[@]}" -alias CARoot -importcert -file ca.cert -storepass "$service-truststore-password" -noprompt

    # Create the key store file. PKCS12 key stores use the store password also for the private key.
    keytool -genkeypair -noprompt "${storetype[@]}" -keystore "$keystore" -alias "$service" -keyalg RSA -keysize "$key_size" -validity "$validity" -storepass "$service-keystore-password" -dname "CN=$service.example.lightstreamer.com, OU=Kafka Connector, O=Lightstreamer Srl, C=IT"

    # Create the CSR file
    keytool -keystore "$keystore" "${storetype[@]}" -alias "$service" -certreq -file "$service/$service.csr" -storepass "$service-keystore-password"

    # Sign the key. The extensions make the certificate usable by strict TLS stacks and allow
    # hostname verification: a service is reachable by its service name (inside the Docker network)
    # and by localhost (from the host).
    cat > "$stage/service-extensions.cnf" <<EOF
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = DNS:$service, DNS:localhost
EOF
    openssl x509 -req -CA ca.cert -CAkey ca.key -in "$service/$service.csr" -out "$service/$service.signed" -days "$validity" -CAcreateserial -extfile "$stage/service-extensions.cnf"

    # Import both the certificate of the CA and the signed certificate back into the key store.
    # The CA certificate is needed only to let keytool build the certificate chain of the signed
    # certificate.
    keytool -keystore "$keystore" "${storetype[@]}" -alias CARoot -importcert -file ca.cert -storepass "$service-keystore-password" -noprompt
    keytool -keystore "$keystore" "${storetype[@]}" -alias "$service" -importcert -file "$service/$service.signed" -storepass "$service-keystore-password" -noprompt

    # Remove the standalone CA entry, as in a production key store: the key store holds only the
    # private key and its certificate chain (signed certificate + CA certificate), while the trust
    # store is the only place where the CAs to trust are listed. Without a trust store, a server
    # would then no longer trust the certificates of the clients signed by the CA.
    keytool -keystore "$keystore" "${storetype[@]}" -alias CARoot -delete -storepass "$service-keystore-password"

    # The CSR and the signed certificate are no longer needed: the key store holds the chain.
    rm -f "$service/$service.csr" "$service/$service.signed"
done

# Save credentials. The broker image requires a key password file, which for a PKCS12 key store
# must contain the key store password.
echo "broker-keystore-password" > broker/broker_keystore_credentials
echo "broker-keystore-password" > broker/broker_key_credentials
echo "broker-truststore-password" > broker/broker_truststore_credentials

# Everything succeeded: replace the existing secrets with the new ones.
cd "$script_dir"
for file in ca.key ca.cert ca.srl; do
    if [[ -f $stage/$file ]]; then
        mv -f "$stage/$file" "$file"
    fi
done
for service in "${services[@]}"; do
    rm -rf "$service"
    mv "$stage/$service" "$service"
done
