package com.basho.riak;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatsRecorder {

	private static HashMap<String, HashMap<Long, Float>> stats = new HashMap<String, HashMap<Long, Float>>();
	private static List<String> ids = new ArrayList<String>();
	
	private final static long ONE_SECOND = 1000;
	
	public static boolean registerID(String id) {
		if (ids.contains(id)) {
			return false;
		} else {
			ids.add(id);
			return true;
		}
	}
	
	public static void recordStat(String id, long time, float value) {
		stats.getOrDefault(id, new HashMap<Long, Float>()).put(time, value);
		HashMap<Long, Float> idStats = stats.get(id);
		if (idStats == null) {
			idStats = new HashMap<Long, Float>();
		}
		idStats.put(time, value);
		stats.put(id, idStats);
	}
	
	public static HashMap<Integer, Float> sumById(String id) {
		HashMap<Long, Float> idStats = stats.get(id);
		long startKey = Collections.min(idStats.keySet());
		long maxKey = Collections.max(idStats.keySet());
		int secondCount = 1;
		HashMap<Integer, Float> summedResults = new HashMap<Integer, Float>();
		while (startKey <= maxKey) {
			long tStart = startKey;
			long tEnd = startKey + ONE_SECOND;
			Map<Long, Float> secondChunk = idStats.entrySet().stream().filter(p -> p.getKey() >= tStart && p.getKey() <= tEnd).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
			long minTKey = Collections.min(secondChunk.keySet());
			long maxTKey = Collections.max(secondChunk.keySet());
			float ops = secondChunk.get(maxTKey) - secondChunk.get(minTKey);
			summedResults.put(secondCount, ops);
			startKey += ONE_SECOND;
			secondCount += 1;
		}
		
		return summedResults;
	}
	
	public static HashMap<Integer, Float> sumAllIds() {
		HashMap<String, HashMap<Integer, Float>> summedById = new HashMap<String, HashMap<Integer, Float>>();
		for (String id : ids) {
			summedById.put(id, sumById(id));
		}
		
		HashMap<Integer, Float> results = new HashMap<Integer, Float>();
		for (int elapsed : summedById.get(ids.get(0)).keySet()) {
			float sum = 0;
			for (String id : ids) {
				if (summedById.get(id).containsKey(elapsed)) {
					sum += summedById.get(id).get(elapsed);
				}
			}
			results.put(elapsed, sum);
		}
		
		return results;
	}
}
