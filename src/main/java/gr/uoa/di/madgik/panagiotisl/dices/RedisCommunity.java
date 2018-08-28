package gr.uoa.di.madgik.panagiotisl.dices;

import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisCommunity implements Serializable {

	private static final long serialVersionUID = 8941749640675016290L;

	private Set<String> seedSet;
	private String[] groundTruth;
	private String id;
	private int intId;
	
	public RedisCommunity(int id, RedisAdvancedClusterCommands<String, String> redisConn, Set<String> seedSet){
		this(id, redisConn, seedSet, null);
	}
	
	public RedisCommunity(int id, RedisAdvancedClusterCommands<String, String> sync, Set<String> seedSet, String[] groundTruh){
		this.intId = id;
		this.id = String.format("RC%d", id);
		if(sync == null)
			throw new IllegalArgumentException();
		if(seedSet == null)
			throw new IllegalArgumentException();
		this.seedSet = new HashSet<>();
		seedSet.forEach(seed -> this.seedSet.add(seed));
		seedSet.forEach(seed-> sync.zadd(this.id, 1D, seed));
		seedSet.forEach(seed-> sync.sadd(String.format("C%s", seed), this.id));
		this.groundTruth = groundTruh;
	}

	public boolean contains(String node, RedisAdvancedClusterCommands<String, String> redisConn) {
		return redisConn.zscore(id, node) != null;
	}
	
	public Double getScore(String node, RedisAdvancedClusterCommands<String, String> redisConn) {
		return redisConn.zscore(id, node);
	}

	public void put(String node, Double value, RedisAdvancedClusterAsyncCommands<String, String> redisAsyncConn) {
		redisAsyncConn.zadd(id, value, node);
		redisAsyncConn.sadd(String.format("C%s", node), id);
	}

	public Double get(String key, RedisAdvancedClusterCommands<String, String> redisConn) {
		return redisConn.zscore(id, key);
	}
	
	public boolean isSeed(String node) {
		return this.seedSet.contains(node);
	}
	
	public String[] getGroundTruth() {
		return this.groundTruth;
	}

	public int size(RedisAdvancedClusterCommands<String, String> redisConn) {
		return redisConn.zcard(id).intValue();
	}
	
	public long pruneCommunity(int size, RedisAdvancedClusterCommands<String, String> redisConn, RedisAdvancedClusterAsyncCommands<String, String> redisAsyncConn) {
		long start = System.nanoTime();
		List<String> toBeRemoved = redisConn.zrevrange(id, size, Long.MAX_VALUE);
		if (!toBeRemoved.isEmpty()) {
			redisAsyncConn.zremrangebyscore(id, 0D, redisConn.zscore(id, toBeRemoved.get(0)));	
		}
		this.seedSet.forEach(seed -> redisAsyncConn.zadd(id, 1D, seed));
		toBeRemoved.forEach((node -> redisAsyncConn.srem(String.format("C%s", node), id)));
		return System.nanoTime() - start;
	}
	
	public List<Entry<String, Double>> getSortedCommunity(RedisAdvancedClusterCommands<String, String> redisConn) {
		return redisConn.zrevrangeWithScores(id, 0, Long.MAX_VALUE).stream()
			.map(scoredValue -> new AbstractMap.SimpleImmutableEntry<String, Double>(scoredValue.getValue(), scoredValue.getScore()))
			.collect(Collectors.toList());
	}

	public Set<String> getPrunedCommunity(int bestSize, RedisAdvancedClusterCommands<String, String> redisConn) {
		return redisConn.zrevrange(id, 0, bestSize).stream()
				.collect(Collectors.toSet());
	}

	public int getId() {
		return this.intId;
	}
	
}
