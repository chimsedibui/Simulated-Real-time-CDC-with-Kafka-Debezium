#!/bin/sh
# SeaweedFS in one process (master, volume, filer, S3 on 8333) with one S3 identity from the environment, and the
# bronze-cold bucket created once; the healthcheck waits for the marker file.
set -eu
cat > /tmp/s3.json <<JSON
{"identities": [{"name": "clickhouse",
                 "credentials": [{"accessKey": "$COLD_STORAGE_ACCESS_KEY", "secretKey": "$COLD_STORAGE_SECRET_KEY"}],
                 "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
JSON
weed server -dir=/data -ip=seaweedfs -ip.bind=0.0.0.0 -volume.max=0 -s3 -s3.port=8333 -s3.config=/tmp/s3.json &
server=$!
until echo "s3.bucket.list" | weed shell -master=seaweedfs:9333 >/dev/null 2>&1; do sleep 1; done
echo "s3.bucket.create -name bronze-cold" | weed shell -master=seaweedfs:9333 >/dev/null 2>&1 || true
touch /data/.bucket-ready
wait $server
