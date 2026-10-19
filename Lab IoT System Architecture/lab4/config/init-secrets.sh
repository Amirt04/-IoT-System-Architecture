#!/bin/sh
# Однократная инициализация: password_file из users.txt (mosquitto_passwd) + ACL с правами 0600.
# Секреты кладутся в именованный том, а не в папку Windows: mosquitto требует, чтобы
# файлы паролей и ACL не были доступны всем (у файлов с диска Windows права 0777).
set -e
umask 077
S=/mosquitto/secrets
mkdir -p "$S"
rm -f "$S/passwd_a" "$S/passwd_b"
touch "$S/passwd_a" "$S/passwd_b"
grep -v '^#' /config/users.txt | grep ':' | tr -d '\r' | while IFS=: read -r broker user pass; do
    mosquitto_passwd -b "$S/passwd_$(echo "$broker" | tr 'AB' 'ab')" "$user" "$pass"
    echo "user $user -> broker $broker"
done
tr -d '\r' < /config/acl_a > "$S/acl_a"
tr -d '\r' < /config/acl_b > "$S/acl_b"
chown -R 1883:1883 "$S"
chmod 0700 "$S"
chmod 0600 "$S"/*
echo "secrets ready:"; ls -l "$S"
