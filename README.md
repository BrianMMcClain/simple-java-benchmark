Build
-----
```
mvn clean package
```

Options
-------
```
 -b,--batch <BATCH SIZE>          How many rows per operation to be
                                  written
 -c,--cassandra                   Run benchmark against Cassandra
 -h,--hosts <HOST1,HOST2,HOST3>   Comma-seperated list of database hosts
 -o,--ops <OPS>                   Number of operations to perform
 -t,--threads <THREADS>           Number of worker threads
 -v,--verbose                     Verbose logging
 ```

 Cassandra Create Column Family
 ------------------------------
 ```
DROP KEYSPACE ycsb; CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}; CREATE TABLE ycsb.usertable (time timestamp PRIMARY KEY, family text, series text, field0 text, field1 text);
 ```