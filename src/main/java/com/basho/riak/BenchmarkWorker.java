package com.basho.riak;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;

public class BenchmarkWorker implements Runnable {

	private int id;
	private String hostname;
	private String[] hosts;
	private int recordCount;
	
	private RiakClient client;
	
	public BenchmarkWorker(int id, String hostname, String[] hosts, int recordCount) {
		this.id = id;
		this.hostname = hostname;
		this.hosts = hosts;
		this.recordCount = recordCount;
	}
	
	public void run() {
		System.err.println("Started worker" + this.id + ", writing " + this.recordCount + " records");
		
		RiakNode.Builder builder = new RiakNode.Builder();
    	List<RiakNode> nodes;
    	try {
    		nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(hosts));
    		RiakCluster riakCluster = new RiakCluster.Builder(nodes).build();
    		riakCluster.start();
    		this.client = new RiakClient(riakCluster);
    	} catch (UnknownHostException e) {
    		e.printStackTrace();
    	}
    	
    	runBenchmarkLoop();		
    	
    	System.err.println("worker" + this.id + " completed without error");
	}
	
	private void runBenchmarkLoop() {
		long timestamp = System.currentTimeMillis();
		int recordsWritten = 0;
		while (recordsWritten < this.recordCount) {
	    	List<Row> rows = Arrays.asList(
	    		    new Row(new Cell(this.hostname), 
	    		    		new Cell("worker" + this.id), 
	    		            Cell.newTimestamp(timestamp), 
	    		            new Cell(1), 
	    		            new Cell("test"),
	    		            new Cell(1.5),
	    		            new Cell(true)
	    		    ));
	
	
	    	Store storeCmd = new Store.Builder("GeoCheckin").withRows(rows).build();
	    	
			try {
				this.client.execute(storeCmd);
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			timestamp += 1;
			recordsWritten += 1;
    	}
	}

}
