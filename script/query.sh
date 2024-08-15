#!/bin/bash

echo "query test:"

curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu1")'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu2")'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu3")'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu4")'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d "from(bucket:\"mybucket\") |> range(start:0) |> filter(fn: (r) => r._measurement == \"measurement with spaces, commas and 'quotes'\")"
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d "from(bucket:\"mybucket\") |> range(start:0) |> filter(fn: (r) => r._measurement == \"'measurement with spaces, commas and 'quotes''\")"
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "measurement with spaces, commas and \"quotes\"")'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "\"measurement with spaces, commas and \"quotes\"\"")'


echo ""
echo "json test:"

curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/json' -d '{"query": "from(bucket:\"mybucket\") |> range(start:0) |> filter(fn: (r) => r._measurement == \"cpu1\")"}'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/json' -d '{"query": "from(bucket:\"mybucket\") |> range(start:0) |> filter(fn: (r) => r._measurement == \"cpu2\")"}'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/json' -d '{"query": "from(bucket:\"mybucket\") |> range(start:0) |> filter(fn: (r) => r._measurement == \"cpu3\")"}'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/json' -d '{"query": "from(bucket:\"mybucket\") |> range(start:0) |> filter(fn: (r) => r._measurement == \"cpu4\")"}'


echo ""
echo "error test:"

curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d ''
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket")'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> range(start:0)'
curl -X POST 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H 'Content-type: application/vnd.flux' -d 'from(bucket:"mybucket") |> filter(fn: (r) => r._measurement == "cpu1")'


echo ""
echo ""
echo "gzip test:"

queries=(
    'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu1")'
    'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu2")'
    'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu3")'
    'from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "cpu4")'
)

len=${#queries[*]}
i=0
while (($i<$len)); do
    query=${queries[$i]}
    curl -X POST -s 'http://127.0.0.1:7076/api/v2/query?org=myorg' -H "Accept-Encoding: gzip" -H 'Content-type: application/vnd.flux' -d "$query" | gzip -d
    i=$(($i+1))
done


echo ""
echo "v1 query test:"

curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show databases'

curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from cpu5;'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=select * from cpu6'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from myrp.cpu7;'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from myrp.cpu8'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode "q=select * from \"measurement v1 with spaces, commas and 'quotes'\""
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode "q=select * from \"'measurement v1 with spaces, commas and 'quotes''\""
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=select * from "measurement v1 with spaces, commas and \"quotes\""'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from "\"measurement v1 with spaces, commas and \"quotes\"\""'

curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show tag keys from cpu5'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show FIELD keys on mydb from cpu6'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show FIELD keys from mydb..cpu6'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show TAG keys from myrp.cpu7'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show field KEYS on mydb from myrp.cpu8'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show field KEYS from mydb.myrp.cpu8'

curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show MEASUREMENTS'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show series'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show series from cpu6'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show field KEYS'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show field KEYS from cpu5'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show TAG keys'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show TAG keys from cpu6'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show tag VALUES WITH key = "region"'
curl -G 'http://127.0.0.1:7076/query?db=mydb&rp=myrp' --data-urlencode 'q=show tag VALUES from cpu6 WITH key = "region"'

curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show MEASUREMENTS on mydb'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show SERIES on mydb'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show SERIES on mydb from myrp.cpu6'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show field KEYS on mydb'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show field KEYS on mydb from myrp.cpu8'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show TAG keys on mydb'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show TAG keys on mydb from myrp.cpu7'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show tag VALUES on mydb WITH key = "region"'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show tag VALUES on mydb from myrp.cpu7 WITH key = "region"'


echo ""
echo "v1 error test:"

curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q='
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * measurement'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show TAG from cpu5'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show TAG values from '
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show field KEYS fr'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show series from'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show measurement'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=show stat'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=drop'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=delete from '
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=drop series'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=drop series from'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=drop measurement'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=CREATE DATABASE'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=drop database '
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show TAG keys test from mem'


echo ""
echo "v1 drop test:"

curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=delete from cpu5'
curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=delete from cpu6'
curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=drop measurement cpu7'
curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=drop measurement cpu8'
curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode "q=delete from \"measurement v1 with spaces, commas and 'quotes'\""
curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode "q=drop measurement \"'measurement v1 with spaces, commas and 'quotes''\""
curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=delete from "measurement v1 with spaces, commas and \"quotes\""'
curl -X POST 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=drop measurement "\"measurement v1 with spaces, commas and \"quotes\"\""'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from cpu5;'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from cpu6'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from myrp.cpu7;'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from myrp.cpu8'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode "q=select * from \"measurement v1 with spaces, commas and 'quotes'\""
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode "q=select * from \"'measurement v1 with spaces, commas and 'quotes''\""
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from "measurement v1 with spaces, commas and \"quotes\""'
curl -G 'http://127.0.0.1:7076/query?db=mydb' --data-urlencode 'q=select * from "\"measurement v1 with spaces, commas and \"quotes\"\""'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show databases'
