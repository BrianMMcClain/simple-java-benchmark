package com.basho.riak;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.*;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.timeseries.*;

public class RiakJavaBenchmark 
{
	
    public static void main( String[] args )
    {
    	String[] hosts = {"127.0.0.1"};
    	RiakNode.Builder builder = new RiakNode.Builder();
    	List<RiakNode> nodes;
    	RiakClient client = null;
    	try {
    		nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(hosts));
    		RiakCluster riakCluster = new RiakCluster.Builder(nodes).build();
    		riakCluster.start();
    		client = new RiakClient(riakCluster);
    	} catch (UnknownHostException e) {
    		e.printStackTrace();
    	}
    	
//    	myfamily varchar not null, 
//    	myseries varchar not null, 
//    	time timestamp not null, 
//    	myint sint64 not null, 
//    	mytext varchar not null, 
//    	myfloat double not null, 
//    	mybool boolean not null,
    	
    	Integer totalCount = 10000;
    	int recordsWritten = 0;
    	long timestamp = System.currentTimeMillis();
    	
    	long startTime = System.currentTimeMillis();
    	while (recordsWritten < totalCount) {
	    	List<Row> rows = Arrays.asList(
	    		    new Row(new Cell("family1"), 
	    		    		new Cell("series1"), 
	    		            Cell.newTimestamp(timestamp), 
	    		            new Cell(1), 
	    		            new Cell("test"),
	    		            new Cell(1.5),
	    		            new Cell(true)
	    		    ));
	
	
	    	Store storeCmd = new Store.Builder("GeoCheckin").withRows(rows).build();
	    	
			try {
				client.execute(storeCmd);
				//System.out.println(timestamp);
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
    	Long endTime = System.currentTimeMillis();
    	
    	Long totalTime = endTime - startTime;
    	float recordsPerSecond = (float) (totalCount / (totalTime / 1000.0));
    	
    	System.out.println("Records Written: " + totalCount);
    	System.out.println("Total Run Time: " + totalTime);
    	System.out.println("Throughput: " + recordsPerSecond);
    }
}
