#!/bin/sh
set -eu

mkdir -p /var/lib/undr9/data
chown undr9:undr9 /var/lib/undr9/data

exec gosu undr9 "$@"
