package com.basho.riak;

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;


public class CustomCsvReporter extends ScheduledReporter {

	private long elapsed = 1;
	private long lastCount = 0;
	private long lastErrorCount = 0;
	private long reportInterval = 1;
	private long startTime = Long.MAX_VALUE;
	private long endTime = 0;
	
	private PrintStream stream;
	
	private final String CSV_HEADERS = "elapsed,ops_per_sec,errors_per_sec,mean,1m_mean,5m_mean,15m_mean,min_latency,max_latency,mean_latency,total_ops,total_errors,time";
	
	private final float NS_IN_MS = 1000000;
	
	protected CustomCsvReporter(MetricRegistry registry, PrintStream stream) {
		super(registry, "metrics", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
		this.stream = stream;

		this.stream.println(CSV_HEADERS);
	}

	@Override
	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
		if (!meters.isEmpty() && !timers.isEmpty()) {
			Meter requests = meters.get("requests");
			Meter errors = meters.get("errors");
			Timer latency = timers.get("latency");
			Snapshot snapshot = latency.getSnapshot();
			
			long time = System.currentTimeMillis();
				
			// Determine if we should report this line
			if ((requests.getCount() - lastCount) > 0 || (errors.getCount() - lastErrorCount) > 0) {
				
				StringBuilder out = new StringBuilder();
				out.append(elapsed).append(",") // Time elapsed
					.append((requests.getCount() - lastCount) / reportInterval).append(",") // Throughput of the last window size
					.append((errors.getCount() - lastErrorCount) / reportInterval).append(",") // Errors
					.append(requests.getMeanRate()).append(",") // Runtime Mean
					.append(requests.getOneMinuteRate()).append(",") // 1m Rate
					.append(requests.getFiveMinuteRate()).append(",") // 5m Rate
					.append(requests.getFifteenMinuteRate()).append(",") // 15m Rate
					.append((float) snapshot.getMin() / NS_IN_MS).append(",") // Min Latency
					.append((float) snapshot.getMax() / NS_IN_MS).append(",") // Max Latency
					.append((float) snapshot.getMean() / NS_IN_MS).append(",") // Mean Latency
					.append(requests.getCount()).append(",") // Total ops count
					.append(errors.getCount()).append(",") // Total error count
					.append(time); // Timestamp, useful for correlation of other metrics
					
				this.stream.println(out.toString());
					
				lastCount = requests.getCount();
				lastErrorCount = errors.getCount();
				elapsed += reportInterval;
				
				if (time < startTime) {
					startTime = time;
				} else if (time > endTime) {
					endTime = time;
				}
			}
		}
	}
	
	@Override
	public void start(long period, TimeUnit unit) {
		elapsed = period;
		reportInterval = period;
		super.start(period, unit);
	}
	
	@Override
	public void close() {
		this.stream.flush();
		this.stream.close();
	}
	
	public long startTime() {
		return startTime;
	}
	
	public long endTime() {
		return endTime;
	}
	
}