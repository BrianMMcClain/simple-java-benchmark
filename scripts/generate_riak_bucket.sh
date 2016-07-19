#!/bin/bash

SIZE="${1:-10}"
COUNT=0
CMD="./riak-admin bucket-type create tsycsb '{\"props\": {\"table_def\": \"CREATE TABLE tsycsb (
		host VARCHAR NOT NULL, 
		worker VARCHAR NOT NULL, 
		time TIMESTAMP NOT NULL"

while [ $COUNT -lt $SIZE ]; do
	CMD="$CMD, field$COUNT VARCHAR"
	let COUNT=COUNT+1
done

CMD="$CMD 
	, primary key ((host, worker, quantum(time, 10, s)), host, worker, time))\"}}';"

echo $CMD

echo "./riak-admin bucket-type activate tsycsb"