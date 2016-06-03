package com.basho.riak;

import java.io.PrintStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;


public class CustomCsvReporter extends ScheduledReporter {

	private int elapsed = 1;
	private long lastCount = 0;
	
	private PrintStream stream;
	
	private final String CSV_HEADERS = "elapsed,throughput,mean,1m_mean,5m_mean,15m_mean,count,time";
	
	protected CustomCsvReporter(MetricRegistry registry, PrintStream stream) {
		super(registry, "metrics", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
		this.stream = stream;

		this.stream.println(CSV_HEADERS);
	}

	@Override
	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
		if (!meters.isEmpty()) {
			for (Entry<String, Meter> entry : meters.entrySet()) {
				Meter m = entry.getValue();
				
				// Determine if we should report this line
				if ((m.getCount() - lastCount) > 0) {
				
					StringBuilder out = new StringBuilder();
					out.append(elapsed).append(",") // Time elapsed
						.append(m.getCount() - lastCount).append(",") // Throughput of the last window size
						.append(m.getMeanRate()).append(",") // Runtime Mean
						.append(m.getOneMinuteRate()).append(",") // 1m Rate
						.append(m.getFiveMinuteRate()).append(",") // 5m Rate
						.append(m.getFifteenMinuteRate()).append(",") // 15m Rate
						.append(m.getCount()).append(",") // Total count
						.append(System.currentTimeMillis()); // Timestamp, useful for correlation of other metrics
					
					this.stream.println(out.toString());
					
					lastCount = m.getCount();
					elapsed++;
				}
			}
		}
	}
	
	@Override
	public void close() {
		this.stream.flush();
		this.stream.close();
	}
	
}