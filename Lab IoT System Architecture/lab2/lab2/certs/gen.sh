#!/bin/sh
# Самоподписанные сертификаты для TLS-listener (создаются один раз)
set -e
cd /certs
[ -f server.crt ] && { echo "certs exist"; exit 0; }
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=lab2-ca" -keyout ca.key -out ca.crt
openssl req -newkey rsa:2048 -nodes -subj "/CN=mosquitto" -keyout server.key -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -out server.crt
chmod 644 *.key *.crt
echo "certs generated"
