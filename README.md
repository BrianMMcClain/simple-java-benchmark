Build
-----
```
mvn clean package
```

Options
-------
```
usage: java -jar simple-java-benchmark
 -b,--batch <BATCH SIZE>          How many rows per operation to be
                                  written
 -c,--cassandra                   Run benchmark against Cassandra
 -h,--hosts <HOST1,HOST2,HOST3>   Comma-seperated list of database hosts
 -n,--colcount <COLUMN COUNT>     Number of columns per row (Default: 10)
 -o,--ops <OPS>                   Number of operations to perform
 -s,--rowsize <ROW SIZE>          Number of bytes per cell (Default: 100)
 -t,--threads <THREADS>           Number of worker threads
 -v,--verbose                     Verbose logging
 ```

 Cassandra Create Column Family
 ------------------------------
 ```
DROP KEYSPACE ycsb; CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}; CREATE TABLE ycsb.usertable (time timestamp PRIMARY KEY, family text, series text, field0 text, field1 text);
 ```

 A helper script has been provided to assist in create a large number of columns

 ```
 $scripts/generate_cassandra_cql.sh 10
DROP KEYSPACE ycsb; CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}; CREATE TABLE ycsb.usertable (time timestamp PRIMARY KEY, family text, series text, field0 text, field1 text, field2 text, field3 text, field4 text, field5 text, field6 text, field7 text, field8 text, field9 text);
```