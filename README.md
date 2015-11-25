Build
-----
```
mvn clean package
```

Run
---
```
java -jar target/riak-java-benchmark-jar-with-dependencies.jar 10.0.0.1,10.0.0.2,10.0.0.3 10000 32
```

Where 

- 10.0.0.1,10.0.0.2,10.0.0.3 = Comma-seperated list of target hosts
- 10000 = Total number of keys to write
- 32 = Worker Pool Size