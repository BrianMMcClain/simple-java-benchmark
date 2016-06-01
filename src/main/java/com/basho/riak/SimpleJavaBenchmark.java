package com.basho.riak;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
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
    private static String outputPath = null;
	
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
	
	/***
	 * Wrapper method for writing to a specified file. If no file is specified, write to stdout
	 * 
	 * @param writer PrintWriter to use for file writing
	 * @param line Line to write
	 */
	private static void writeLine(BufferedWriter writer, String line) {
		if (writer == null) {
			System.out.println(line);
		} else {
			try {
				writer.write(line);
				writer.newLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
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
        log.info("Row Size: " + rowSize);
    	log.info("Threads: " + workerPoolSize);

    	// Setup and execute all worker threads
    	ExecutorService executor = Executors.newFixedThreadPool(workerPoolSize);
    	log.info("Starting " + workerPoolSize + " threads writing " + (recordCount / workerPoolSize) + " operations each");
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
    	
    	// Write output to defined file. If no file is defined, write to stdout
    	BufferedWriter writer = null;
    	if (outputPath != null) {
	    	try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	HashMap<Integer, Float> summedResults = StatsRecorder.sumAllIds();
    	SortedSet<Integer> keys = new TreeSet<Integer>();
    	keys.addAll(summedResults.keySet()); 
    	float sum = 0f;
    	
    	writeLine(writer, "elapsed,throughput");
    	for (int k : keys) {
    		StringBuilder b = new StringBuilder();
    		b.append(k).append(",").append(summedResults.get(k));
    		writeLine(writer, b.toString());
    		sum += summedResults.get(k);
    	}

    	// Flush writer, if we have one
    	if (writer != null) {
    		try {
				writer.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	Long totalTime = endTime - startTime;
    	float recordsPerSecond = sum / keys.size();
    	
    	log.info("Records Written: " + sum);
    	log.info("Total Run Time: " + totalTime + " ms");
    	log.info("Throughput: " + recordsPerSecond + " ops/s");
    	
    	System.exit(0);
    }
}
