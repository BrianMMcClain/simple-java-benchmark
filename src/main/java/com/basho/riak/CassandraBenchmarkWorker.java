// CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}; create table ycsb.usertable (time timestamp primary key, family varchar, series varchar, field0 varchar, field1 varchar, field2 varchar, field3 varchar, field4 varchar, field5 varchar, field6 varchar, field7 varchar, field8 varchar, field9 varchar);

package com.basho.riak;

import java.util.Random;
import java.util.logging.Logger;

import com.basho.riak.client.api.commands.timeseries.Query;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraBenchmarkWorker implements Runnable {

	private int id;
	private String hostname;
	private String[] hosts;
	private int recordCount;
	private int batchSize;
	
	private Random rand;
	
	private final String KEYSPACE_NAME = "ycsb";
	private final String TABLE_NAME = "usertable";
	private int colCount = 10;
	private int rowSize = 100;
	private int queryRange = -10;
	private int queryLimit = 10;

	private Cluster cluster;
	private Session session;
	
	private Logger log;

	private Meter requests;
	private Meter errors;
	private Timer latency;
	
	public CassandraBenchmarkWorker(int id, String hostname, String[] hosts, int recordCount, int batchSize, int colCount, int rowSize, int queryRange, int queryLimit, Logger log, Meter requestsMeter, Meter errorsMeter, Timer latencyMeter) {
		this.id = id;
		this.hostname = hostname;
		this.hosts = hosts;
		this.recordCount = recordCount;
		this.batchSize = batchSize;
		this.colCount = colCount;
		this.rowSize = rowSize;
		this.queryRange = queryRange;
		this.queryLimit = queryLimit;
		this.rand = new Random(System.currentTimeMillis());
		this.log = log;
		this.requests = requestsMeter;
		this.errors = errorsMeter;
		this.latency = latencyMeter;
	}
	
	@Override
	public void run() {
		log.fine("Started cassandra worker" + this.id + ", writing " + this.recordCount + " records");
		
		// Connect the Cassandra client
		cluster = Cluster.builder().addContactPoints(hosts).build();
		session = cluster.connect(KEYSPACE_NAME);
		
		if (this.queryRange < 0) {
			runBenchmarkLoop();
		} else {
			runQueryBenchmarkLoop();
		}
    	
    	session.close();
    	cluster.close();
    	
    	log.fine("worker" + this.id + " completed without error");
	}

	private void runBenchmarkLoop() {
		long timestamp = 1;
		float recordsWritten = 0;
		while (recordsWritten < this.recordCount) {			
			Insert insertStatement = generateYCSBStatement(timestamp, this.batchSize);
			insertStatement.setConsistencyLevel(ConsistencyLevel.ONE);
			try {
				Timer.Context context = latency.time();
				session.execute(insertStatement);
				requests.mark();
				context.stop();
			} catch (Exception e) {
				log.warning(e.getMessage());
				errors.mark();
			}
			
			timestamp += this.batchSize;
			recordsWritten += 1;
    	}
	}
	
	private void runQueryBenchmarkLoop() {
		int queriesIssues = 0;
		while (queriesIssues < this.queryLimit) {
			String queryString = generateYCSBQuery(1, this.recordCount, this.queryRange);
			try {
				Timer.Context context = latency.time();
				ResultSet results = session.execute(queryString);
				context.stop();
				requests.mark();
			} catch (Exception e) {
				log.warning(e.getMessage());
				errors.mark();
			}
			queriesIssues++;
		}
	}

//	private List<Row> generateAllTypeValue(long startTimestamp, int batchSize) {
//		long timestamp = startTimestamp;
//		List<Row> batch = new ArrayList<Row>();
//		
//		for (int i = 0; i < batchSize; i++) {
//			batch.add(new Row(
//					new Cell(this.hostname),
//					new Cell("worker" + this.id), 
//		            Cell.newTimestamp(timestamp), 
//		            new Cell(1), 
//		            new Cell("test"),
//		            new Cell(1.5),
//		            new Cell(true)
//			));
//			timestamp++;
//		}
//		
//		return batch;
//	}

	private Insert generateYCSBStatement(long startTimestamp, int batchSize) {
		long timestamp = startTimestamp;
		
		Insert insertStatement = QueryBuilder.insertInto(TABLE_NAME);
		insertStatement.value("time", timestamp);
		insertStatement.value("family", this.hostname);
		insertStatement.value("series", "worker" + this.id);
		
		byte buffer [] = new byte[this.rowSize];
		for (int j = 0; j < colCount; j++) {
			rand.nextBytes(buffer);
			insertStatement.value("field" + j, new String(buffer));
		}
		
		timestamp++;
		
		return insertStatement;
	}
	
	private String generateYCSBQuery(long startTimestamp, long endTimestamp, int count)
	{
		long start = startTimestamp + (long)(rand.nextInt((int)(endTimestamp - startTimestamp - count)));
		long end = start + count;
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ")
		  .append(this.TABLE_NAME)
		  .append(" WHERE ")
		  .append("family = '").append(this.hostname).append("'")
		  .append(" AND " )
		  .append("series = '").append("worker" + this.id).append("'")
		  .append(" AND " )
		  .append("time >= ").append(start)
		  .append(" AND ")
		  .append("time < ").append(end)
		  .append(";");
		
		return sb.toString();
	}
}
