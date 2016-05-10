package com.basho.riak;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	
	private static Logger log = Logger.getLogger("");
	
    public static void main( String[] args )
    {    	    	
    	// Parse CLI flags
    	CommandLineParser parser = new DefaultParser();
    	Options options = new Options();

    	options.addOption(Option.builder("h").longOpt("hosts").hasArg().argName("HOST1,HOST2,HOST3").desc("Comma-seperated list of database hosts").required().build());
    	options.addOption(Option.builder("o").longOpt("ops").hasArg().argName("OPS").desc("Number of operations to perform").required().build());
    	options.addOption(Option.builder("t").longOpt("threads").hasArg().argName("THREADS").desc("Number of worker threads").required().build());
    	options.addOption(Option.builder("b").longOpt("batch").hasArg().argName("BATCH SIZE").desc("How many rows per operation to be written").required().build());
    	options.addOption(Option.builder("v").longOpt("verbose").desc("Verbose logging").build());
    	// TODO Add "help" option
    	
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
    	
    	// Setup the logger
    	log.getHandlers()[0].setFormatter(new LoggerFormatter());
    	if (line.hasOption('v')) {
    		log.setLevel(Level.CONFIG);
    		log.getHandlers()[0].setLevel(Level.CONFIG);
    	} else {
    		log.setLevel(Level.INFO);
    		log.getHandlers()[0].setLevel(Level.INFO);
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
    	log.info("Threads: " + workerPoolSize);

    	ExecutorService executor = Executors.newFixedThreadPool(workerPoolSize);
    	log.info("Starting " + workerPoolSize + " threads writing " + (recordCount / workerPoolSize) + " operations");
    	long startTime = System.currentTimeMillis();
    	for (int i = 0; i < workerPoolSize; i++) {
    		Runnable worker = new RiakBenchmarkWorker(i, hostname, hosts, recordCount / workerPoolSize, batchSize, log);
    		executor.execute(worker);
    	}
    	executor.shutdown();
    	
    	while(!executor.isTerminated()) {
    		try {
				executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	long endTime = System.currentTimeMillis();
    	
    	Long totalTime = endTime - startTime;
    	float recordsPerSecond = (float) (recordCount / (totalTime / 1000.0));
    	
    	log.info("Records Written: " + recordCount);
    	log.info("Total Run Time: " + totalTime + " ms");
    	log.info("Throughput: " + recordsPerSecond + " ops/s");
    	
    	System.exit(0);
    }
}
