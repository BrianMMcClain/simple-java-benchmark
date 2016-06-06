package com.basho.riak;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public class RiakBenchmarkWorker implements Runnable {

	private int id;
	private String hostname;
	private String[] hosts;
	private int recordCount;
	private int batchSize;
	
	private Random rand;
	
	private final String BUCKET_NAME = "tsycsb";
	private int colCount = 10;
	private int rowSize = 100;

	private RiakClient client;
	
	private Logger log;

	private Meter requests;
	private Meter errors;
	private Timer latency;
	
	public RiakBenchmarkWorker(int id, String hostname, String[] hosts, int recordCount, int batchSize, int colCount, int rowSize, Logger log, Meter requestsMeter, Meter errorsMeter, Timer latencyMeter) {
		this.id = id;
		this.hostname = hostname;
		this.hosts = hosts;
		this.recordCount = recordCount;
		this.batchSize = batchSize;
		this.colCount = colCount;
		this.rowSize = rowSize;
		this.rand = new Random(System.currentTimeMillis());
		this.log = log;
		this.requests = requestsMeter;
		this.errors = errorsMeter;
		this.latency = latencyMeter;
	}
	
	public void run() {
		log.config("Started riak worker" + this.id + ", writing " + this.recordCount + " records");
		
		RiakNode.Builder builder = new RiakNode.Builder();
    	List<RiakNode> nodes;
    	try {
    		nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(hosts));
    		RiakCluster riakCluster = new RiakCluster.Builder(nodes).build();
    		riakCluster.start();
    		this.client = new RiakClient(riakCluster);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
    	runBenchmarkLoop();		
    	
    	log.config("worker" + this.id + " completed without error");
	}
	
	private void runBenchmarkLoop() {
		long timestamp = System.currentTimeMillis();
		int recordsWritten = 0;
		while (recordsWritten < this.recordCount) {
	    	List<Row> rows = generateYCSBValue(timestamp, this.batchSize);
	    	Store storeCmd = new Store.Builder(BUCKET_NAME).withRows(rows).build();
			try {
				Timer.Context context = latency.time();
				this.client.execute(storeCmd);
				context.stop();
				requests.mark();
			} catch (Exception e) {
				log.warning(e.getMessage());
				errors.mark();
			} 
			
			timestamp += this.batchSize;
			recordsWritten += 1;
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

	private List<Row> generateYCSBValue(long startTimestamp, int batchSize) {
		long timestamp = startTimestamp;
		List<Row> batch = new ArrayList<Row>();
		
		byte buffer [] = new byte[this.rowSize];
		
		for (int i = 0; i < batchSize; i++) {
			List<Cell> cells = new ArrayList<Cell>();
			cells.add(new Cell(this.hostname));
			cells.add(new Cell("worker" + this.id));
			cells.add(Cell.newTimestamp(timestamp));
			for (int j = 0; j < this.colCount; j++) {
				rand.nextBytes(buffer);
				cells.add(new Cell(new String(buffer)));
			}
			batch.add(new Row(cells));
			timestamp++;
		}
		
		return batch;
	}
}
