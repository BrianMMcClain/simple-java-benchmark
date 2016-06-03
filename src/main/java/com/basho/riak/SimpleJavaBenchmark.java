package com.basho.riak;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class SimpleJavaBenchmark 
{
	private static String[] hosts = {"127.0.0.1"};
	private static int recordCount = 10000;
	private static int workerPoolSize = 64;
	private static int batchSize = 1;
    private static int colCount = 10;
    private static int rowSize = 100;
    private static int reportInterval = 1;
    private static String outputPath = null; 
    
	private static Logger log = Logger.getLogger("");
	
	static final MetricRegistry metrics = new MetricRegistry();
	static Meter requestsMeter = metrics.meter("requests");
	static Meter errorsMeter = metrics.meter("errors");
		
    public static void main( String[] args )
    {
    	// Setup CLI flag parser
     	CommandLineParser parser = new DefaultParser();
    	Options options = new Options();

    	options.addOption(Option.builder("h").longOpt("hosts").hasArg().argName("HOST1,HOST2,HOST3").desc("Comma-seperated list of database hosts").required().build());
    	options.addOption(Option.builder("r").longOpt("records").hasArg().argName("RECORDS").desc("Number of operations to perform").required().build());
    	options.addOption(Option.builder("t").longOpt("threads").hasArg().argName("THREADS").desc("Number of worker threads").required().build());
    	options.addOption(Option.builder("b").longOpt("batch").hasArg().argName("BATCH SIZE").desc("How many rows per operation to be written").required().build());
        options.addOption(Option.builder("n").longOpt("colcount").hasArg().argName("COLUMN COUNT").desc("Number of columns per row (Default: 10)").build());
        options.addOption(Option.builder("s").longOpt("rowsize").hasArg().argName("ROW SIZE").desc("Number of bytes per cell (Default: 100)").build());
    	options.addOption(Option.builder("v").longOpt("verbose").desc("Verbose logging").build());
    	options.addOption(Option.builder("c").longOpt("cassandra").desc("Run benchmark against Cassandra").build());
    	options.addOption(Option.builder("o").longOpt("output").hasArg().argName("OUTPUT PATH").desc("Path to write CSV output").build());
    	options.addOption(Option.builder("a").longOpt("table").hasArg().argName("TABLE NAME").desc("Bucket/Column Family").build());
    	options.addOption(Option.builder("i").longOpt("interval").hasArg().argName("REPORT INTERVAL").desc("CSV Report Interval").build());
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
    	
    	hosts = line.getOptionValue("h","127.0.0.1").split(",");
    	recordCount = Integer.parseInt(line.getOptionValue("r", "1000"));
    	workerPoolSize = Integer.parseInt(line.getOptionValue("t", "1"));
    	batchSize = Integer.parseInt(line.getOptionValue("b", "1"));
        colCount = Integer.parseInt(line.getOptionValue("n", "3"));
        rowSize = Integer.parseInt(line.getOptionValue("s", "10"));
        outputPath = line.getOptionValue("o", null);
        reportInterval = Integer.parseInt(line.getOptionValue("i", "1"));

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

    	log.info("Hosts: ");
    	for (String host : hosts) {
    		log.info("   " + host);
    	}
    	log.info("Record Count: " + recordCount);
    	log.info("Batch Size: " + batchSize);
        log.info("Column Count: " + colCount);
        log.info("Cell Size: " + rowSize);
    	log.info("Threads: " + workerPoolSize);

    	// Setup the metrics reporter
    	PrintStream stream = System.out;
    	if (outputPath != null) {
    		try {
				stream = new PrintStream(new File(outputPath));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	CustomCsvReporter reporter = new CustomCsvReporter(metrics, stream);
 		reporter.start(reportInterval, TimeUnit.SECONDS);
    	
    	// Setup and execute all worker threads
    	ExecutorService executor = Executors.newFixedThreadPool(workerPoolSize);
    	log.info("Starting " + workerPoolSize + " threads writing " + (recordCount / workerPoolSize) + " operations each");
    	for (int i = 0; i < workerPoolSize; i++) {
    		Runnable worker;
    		if (cassandraTest) {
    			worker = new CassandraBenchmarkWorker(i, hostname, hosts, recordCount / workerPoolSize, batchSize, colCount, rowSize, log, requestsMeter, errorsMeter);
    		} else {
    			worker = new RiakBenchmarkWorker(i, hostname, hosts, recordCount / workerPoolSize, batchSize, log);
    		}
    		executor.submit(worker);
    	}
    	
    	// Wait for all worker threads to complete execution
    	executor.shutdown();
    	while(!executor.isTerminated()) {
    		try {
				executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	 	
    	reporter.stop();
    	reporter.close();
    	
    	log.info("Records Written: " + requestsMeter.getCount());
    	log.info("Throughput: " + requestsMeter.getMeanRate());
    	
    	System.exit(0);
    }
}
