package com.basho.riak;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RiakJavaBenchmark 
{
	private static String[] hosts = {"127.0.0.1"};
	private static int recordCount = 100000;
	private static int workerPoolSize = 32;
	
    public static void main( String[] args )
    {    	
//    	myfamily varchar not null, 
//    	myseries varchar not null, 
//    	time timestamp not null, 
//    	myint sint64 not null, 
//    	mytext varchar not null, 
//    	myfloat double not null, 
//    	mybool boolean not null,
    	
    	long startTime = System.currentTimeMillis();
    	
    	ExecutorService executor = Executors.newFixedThreadPool(workerPoolSize);
    	for (int i = 0; i < workerPoolSize; i++) {
    		Runnable worker = new BenchmarkWorker(i, hosts, recordCount / workerPoolSize);
    		executor.execute(worker);
    	}
    	executor.shutdown();
    	while(!executor.isTerminated()) {
    		
    	}
    	
    	long endTime = System.currentTimeMillis();
    	
    	Long totalTime = endTime - startTime;
    	float recordsPerSecond = (float) (recordCount / (totalTime / 1000.0));
    	
    	System.out.println("Records Written: " + recordCount);
    	System.out.println("Total Run Time: " + totalTime);
    	System.out.println("Throughput: " + recordsPerSecond);
    }
}
