package com.basho.riak;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RiakJavaBenchmark 
{
	private static String[] hosts = {"127.0.0.1"};
	private static int recordCount = 10000;
	private static int workerPoolSize = 64;
	
    public static void main( String[] args )
    {    	    	
    	if (args.length != 3) {
    		System.out.println("Usage: RiakJavaBenchmark HOSTS RECORD_COUNT WORKER_COUNT");
    		System.exit(-1);
    	}
    	
    	hosts = args[0].split(",");
    	recordCount = new Integer(args[1]);
    	workerPoolSize = new Integer(args[2]);
    	String hostname = "localhost";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	System.out.println("Hosts:");
    	for (int i = 0; i < hosts.length; i++) {
    		System.out.println("   " + hosts[i]);
    	}
    	System.out.println("Records: " + recordCount);
    	System.out.println("Workers: " + workerPoolSize);
    	
    	long startTime = System.currentTimeMillis();
    	
    	ExecutorService executor = Executors.newFixedThreadPool(workerPoolSize);
    	for (int i = 0; i < workerPoolSize; i++) {
    		Runnable worker = new BenchmarkWorker(i, hostname, hosts, recordCount / workerPoolSize);
    		executor.execute(worker);
    	}
    	executor.shutdown();
    	while(!executor.isTerminated()) {
    		
    	}
    	
    	long endTime = System.currentTimeMillis();
    	
    	Long totalTime = endTime - startTime;
    	float recordsPerSecond = (float) (recordCount / (totalTime / 1000.0));
    	
    	System.out.println("Records Written: " + recordCount);
    	System.out.println("Total Run Time: " + totalTime + " ms");
    	System.out.println("Throughput: " + recordsPerSecond + " ops/s");
    	
    	System.exit(0);
    }
}
