#!/bin/bash

if [[ $# -gt 1 ]] || [[ "$1" != "" && "$1" != "https" ]]; then
    echo "Usage: $0 [https]"
    exit 1
fi

if [[ "$1" == "https" ]]; then
    echo "Mode: $1"
    OPTIONS="-v ${PWD}/cert:/cert -e INFLUXD_TLS_CERT=/cert/tls.crt -e INFLUXD_TLS_KEY=/cert/tls.key"
else
    echo "Mode: default"
fi

docker run -d --name influxdb-1 -p 8086:8086 ${OPTIONS} influxdb:2.7
docker run -d --name influxdb-2 -p 8087:8086 ${OPTIONS} influxdb:2.7
docker run -d --name influxdb-3 -p 8088:8086 ${OPTIONS} influxdb:2.7
docker run -d --name influxdb-4 -p 8089:8086 ${OPTIONS} influxdb:2.7


BASEDIR=$(cd $(dirname $0)/..; pwd)

function sedx() {
    if [[ -n $(sed --version 2> /dev/null | grep "GNU sed") ]]; then
        sed -i "$@"
    else
        sed -i '' "$@"
    fi
}

function setup() {
    until docker logs influxdb-$1 2>&1 | grep ':8086' &>/dev/null; do
        counter=$((counter+1))
        if [ $counter -eq 30 ]; then
            echo "error: influxdb is not ready"
            exit 1
        fi
        sleep 0.5
    done
    # setup
    docker exec -it influxdb-$1 influx setup -u influxdb -p influxdb -o myorg -b mybucket -f &> /dev/null
    # dbrp mapping
    BUCKET_ID=$(docker exec -it influxdb-$1 influx bucket list -n mybucket --hide-headers | cut -f 1)
    docker exec -it influxdb-$1 influx v1 dbrp create --db mydb --rp myrp --bucket-id ${BUCKET_ID} --default &> /dev/null
    # set token
    INFLUX_TOKEN=$(docker exec -it influxdb-$1 influx auth list -u influxdb --hide-headers | cut -f 3)
    sedx "$2s#\"token\": \".*\"#\"token\": \"${INFLUX_TOKEN}\"#" ${BASEDIR}/proxy.json
}

sedx '36s#"mapping": {.*}#"mapping": {"mydb": "myorg/mybucket", "mydb/myrp": "myorg/mybucket"}#' ${BASEDIR}/proxy.json

setup 1 9
setup 2 14
setup 3 24
setup 4 29
