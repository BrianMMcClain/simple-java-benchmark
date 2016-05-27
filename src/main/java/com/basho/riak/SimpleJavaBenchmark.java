package com.basho.riak;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class SimpleJavaBenchmark 
{
	private static String[] hosts = {"127.0.0.1"};
	private static int recordCount = 10000;
	private static int workerPoolSize = 64;
	private static int batchSize = 1;
    private static int colCount = 10;
    private static int rowSize = 100;
	
	private static Logger log = Logger.getLogger("");

	/***
	 * Check that all Futures are completed
	 *  
	 * @param set Set of Futures for each worker thread
	 * @return true if all Futures are complete, otherwise returns false
	 */
	private static boolean allFuturesComplete(Set<Future<HashMap<Float, Float>>> set) {
		Iterator<Future<HashMap<Float, Float>>> i = set.iterator();
		while (i.hasNext()) {
			Future<HashMap<Float, Float>> f = i.next();
			if (!f.isDone()) {
				return false;
			}
		}
		
		return true;
    }
	
    public static void main( String[] args )
    {    	    	
    	// Setup CLI flag parser
    	CommandLineParser parser = new DefaultParser();
    	Options options = new Options();

    	options.addOption(Option.builder("h").longOpt("hosts").hasArg().argName("HOST1,HOST2,HOST3").desc("Comma-seperated list of database hosts").required().build());
    	options.addOption(Option.builder("o").longOpt("ops").hasArg().argName("OPS").desc("Number of operations to perform").required().build());
    	options.addOption(Option.builder("t").longOpt("threads").hasArg().argName("THREADS").desc("Number of worker threads").required().build());
    	options.addOption(Option.builder("b").longOpt("batch").hasArg().argName("BATCH SIZE").desc("How many rows per operation to be written").required().build());
        options.addOption(Option.builder("n").longOpt("colcount").hasArg().argName("COLUMN COUNT").desc("Number of columns per row (Default: 10)").build());
        options.addOption(Option.builder("s").longOpt("rowsize").hasArg().argName("ROW SIZE").desc("Number of bytes per cell (Default: 100)").build());
    	options.addOption(Option.builder("v").longOpt("verbose").desc("Verbose logging").build());
    	//options.addOption(Option.builder("r").longOpt("riak").desc("Run benchmark against Riak (default)").build());
    	options.addOption(Option.builder("c").longOpt("cassandra").desc("Run benchmark against Cassandra").build());
    	// TODO Add "help" option
    	
    	// Parse CLI flags, showing help if needed
    	CommandLine line = null;
    	try {
			line = parser.parse(options, args);
		} catch (ParseException e1) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar simple-java-benchmark", options);
			System.exit(-1);
		}
    	
    	hosts = line.getOptionValue("h").split(",");
    	recordCount = Integer.parseInt(line.getOptionValue("o"));
    	workerPoolSize = Integer.parseInt(line.getOptionValue("t"));
    	batchSize = Integer.parseInt(line.getOptionValue("b"));
        colCount = Integer.parseInt(line.getOptionValue("n"));
        rowSize = Integer.parseInt(line.getOptionValue("s"));

    	// Setup the logger
    	log.getHandlers()[0].setFormatter(new LoggerFormatter());
    	if (line.hasOption('v')) {
    		log.setLevel(Level.FINE);
    		log.getHandlers()[0].setLevel(Level.FINE);
    	} else {
    		log.setLevel(Level.INFO);
    		log.getHandlers()[0].setLevel(Level.INFO);
    	}
    	
    	boolean cassandraTest = false;
    	if (line.hasOption('c')) {
    		cassandraTest = true;
    	}
    	
    	// Attempt to get the machine's hostname
    	String hostname = "localhost";
    	try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	log.info("Hosts: " + line.getOptionValue("h"));
    	log.info("Record Count: " + recordCount);
    	log.info("Batch Size: " + batchSize);
        log.info("Column Count: " + colCount);
        log.info("Row Size: " + rowSize);
    	log.info("Threads: " + workerPoolSize);

    	// Setup and execute all worker threads
    	ExecutorService executor = Executors.newFixedThreadPool(workerPoolSize);
    	log.info("Starting " + workerPoolSize + " threads writing " + (recordCount / workerPoolSize) + " operations");
    	System.out.println("elapsed,throughput");
    	long startTime = System.currentTimeMillis();
    	Set<Future<HashMap<Float, Float>>> results = new HashSet<Future<HashMap<Float, Float>>>();
    	for (int i = 0; i < workerPoolSize; i++) {
    		Callable<HashMap<Float, Float>> worker;
    		if (cassandraTest) {
    			worker = new CassandraBenchmarkWorker(i, hostname, hosts, recordCount / workerPoolSize, batchSize, colCount, rowSize, log);
    		} else {
    			worker = new RiakBenchmarkWorker(i, hostname, hosts, recordCount / workerPoolSize, batchSize, log);
    		}
    		Future<HashMap<Float, Float>> result = executor.submit(worker);
    		results.add(result);
    	}
    	
    	// Wait for all worker threads to complete execution
    	while(!allFuturesComplete(results)) {
    		try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	long endTime = System.currentTimeMillis();
    	
    	HashMap<Integer, Float> summedResults = StatsRecorder.sumAllIds();
    	SortedSet<Integer> keys = new TreeSet<Integer>();
    	keys.addAll(summedResults.keySet()); 
    	float sum = 0f;
    	for (int k : keys) {
    		System.out.println(k + "," + summedResults.get(k));
    		sum += summedResults.get(k);
    	}

    	Long totalTime = endTime - startTime;
    	float recordsPerSecond = sum / keys.size();
    	
    	log.info("Records Written: " + sum);
    	log.info("Total Run Time: " + totalTime + " ms");
    	log.info("Throughput: " + recordsPerSecond + " ops/s");
    	
    	System.exit(0);
    }
}
