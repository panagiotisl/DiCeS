package gr.uoa.di.madgik.panagiotisl.dices;

import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class DiCeS {

	private static final Logger LOGGER = Logger.getLogger(DiCeS.class);
	
	public static int BOLTS = 4;
	
	public static String REDIS_CONNECTION;
	
	public static List<Integer> REDIS_PORTS;
	
	public static String GRAPH_FILE;

	public static String GROUND_TRUTH_FILE;
	
	public static String GRAPH_FILE_DELIMITER = " ";
	
	public static String GROUND_TRUTH_FILE_DELIMITER = " ";

	public enum SizeDetermination {
		GROUND_TRUTH, DROP_TAIL
	};

	public static final int MAX_COMMUNITY_SIZE = 100;
	private static final Random RANDOM = new Random(23);
	public static final int NUMBER_OF_SEEDS = 3;
	public static final int WINDOW_SIZE = 10000;
	private static final String NODE_COMMUNITIES_PREFIX = "C%s";

	public static SizeDetermination SIZE_DETERMINATION;

	public void execute(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		TopologyBuilder tb = new TopologyBuilder();

		tb.setSpout("DiCeS-Spout", new DiCeSSpout());

		tb.setBolt("DiCeS-Bolt", new DiCeSBolt(), BOLTS)
				.customGrouping("DiCeS-Spout", new DiCeSGrouping());
		
		tb.setBolt("Pruning-Bolt", new PruningBolt())
				.allGrouping("DiCeS-Bolt");
		
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_DEBUG, false);
		conf.setDebug(false);
		conf.setMessageTimeoutSecs(1000);
		conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 4096);
		conf.setMaxSpoutPending(15000);
		
		if (args != null && args.length > 0) {
			
			LOGGER.info("Submitting topology to remote cluster...");
			
			conf.setNumWorkers(BOLTS + 2);
            conf.setNumAckers(1);
            StormSubmitter.submitTopology(args[0], conf, tb.createTopology());
		} else {
			
			LOGGER.info("Submitting topology to local cluster...");
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("DiCeSTopology", conf, tb.createTopology());	
						
		}
		
	}

	
	public static Set<Integer> getRandomNumbers(int max, int numbersNeeded) {
		if (max < numbersNeeded) {
			throw new IllegalArgumentException("Can't ask for more numbers than are available");
		}
		// Note: use LinkedHashSet to maintain insertion order
		Set<Integer> generated = new LinkedHashSet<Integer>();
		while (generated.size() < numbersNeeded) {
			Integer next = RANDOM.nextInt(max);
			// As we're adding to a set, this will automatically do a containment check
			generated.add(next);
		}
		return generated;
	}
	
	public static double getGroundTruthSizeDetermination(RedisCommunity community, RedisAdvancedClusterCommands<String, String> sync) {
		
		int bestSize = Math.min(community.getGroundTruth().length, community.size(sync));
		double f1score = 0D;
		try {
			f1score = getF1Score(community.getPrunedCommunity(bestSize, sync), community.getGroundTruth());
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return f1score;
	}
	
	public static double getDropTailSizeDetermination(RedisCommunity community, RedisAdvancedClusterCommands<String, String> redisConn) {
		int bestSize = 0;
		List<Entry<String, Double>> sortedCommunity = community.getSortedCommunity(redisConn);
		LOGGER.info(sortedCommunity);
		// calculate mean distance
		Double previous = null;
		double totalDifference = 0D;
		int totalDifferenceCount = 0;
		for (int i = 2 * NUMBER_OF_SEEDS; i < sortedCommunity.size(); i++) {
			Entry<String, Double> entry = sortedCommunity.get(i);
			if (previous != null && !community.isSeed(entry.getKey())) {
				totalDifference += previous - entry.getValue();
				totalDifferenceCount++;
			}
			previous = entry.getValue();
		}
		double meanDifference = totalDifference / totalDifferenceCount;

		// TODO redis can do it as well
		List<Entry<String, Double>> reverseSortedCommunity = community.getSortedCommunity(redisConn);
		Collections.reverse(reverseSortedCommunity);
		previous = null;
		int tailSize = 0;
		for (Entry<String, Double> entry : reverseSortedCommunity) {
			tailSize++;
			if (previous != null) {
				double difference = entry.getValue() - previous;
				if (difference > meanDifference) {
					break;
				}
			}
			previous = entry.getValue();
		}

		bestSize = community.size(redisConn) - tailSize;
		LOGGER.info(community.getId() + ": GT: " + community.getGroundTruth().length + " DT: " + bestSize);
		double f1score = 0D;
		try {
			f1score = getF1Score(community.getPrunedCommunity(bestSize,redisConn), community.getGroundTruth());
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return f1score;
	}

	private static double getPrecision(Set<String> found, Set<String> gtc,
			SetView<String> common) {
		return (double) common.size() / found.size();
	}

	private static double getRecall(Set<String> found, Set<String> gtc,
			SetView<String> common) {
		return (double) common.size() / gtc.size();
	}

	private static double getF1Score(Set<String> found, String[] comm) {
		HashSet<String> gtc = new HashSet<String>(Arrays.asList(comm));
		SetView<String> common = Sets.intersection(found, gtc);
		double precision = getPrecision(found, gtc, common);
		double recall = getRecall(found, gtc, common);
		if (precision == 0 && recall == 0)
			return 0;
		else
			return 2 * (precision * recall) / (precision + recall);
	}

	public static void addToDegreeRedis(String[] nodes, RedisAdvancedClusterAsyncCommands<String, String> redisConnection) {
		redisConnection.incr(nodes[0]);
		redisConnection.incr(nodes[1]);
		redisConnection.incrby(DiCeSBolt.DEGREES_SUM, 2L);
	}
	

	public static void addToCommRedisPRV(String[] nodes,
			RedisAdvancedClusterCommands<String, String> redisConnection,
			RedisAdvancedClusterAsyncCommands<String, String> redisAsyncConnection,
			List<RedisCommunity> communities) {
		
		
		Set<String> set0 = redisConnection.smembers(String.format(NODE_COMMUNITIES_PREFIX, nodes[0]));
		Set<String> set1 = redisConnection.smembers(String.format(NODE_COMMUNITIES_PREFIX, nodes[1]));
		Set<String> validCommunities = Sets.union(set0, set1);
		for (String communityIndex : validCommunities) {
			int i = Integer.valueOf(communityIndex.substring(2));
			Double scoreNode0 = null;
			Double scoreNode1 = null;
			boolean commContainsNode0 = false;
			boolean commContainsNode1 = false;
			
			double commDegreeSum = 0D;
			double degreeSum = 0D;
			
//			String commDegreeSumString = redisConnection.get(DiCeSBolt.COMMUNITY_DEGREES_SUM);
//			String degreeSumString = redisConnection.get(DiCeSBolt.DEGREES_SUM);
//			if (commDegreeSumString != null) {
//				commDegreeSum = Double.parseDouble(commDegreeSumString) / 150_000;
//			}
//			if (degreeSumString != null) {
//				degreeSum = Double.parseDouble(degreeSumString) / 150_000;
//			}
			
			RedisCommunity comm = communities.get(i);
			if (set0.contains(communityIndex)) {
				scoreNode0 = comm.getScore(nodes[0], redisConnection);
//				LOGGER.warn("scorenode0: " + scoreNode0);
				commContainsNode0 = scoreNode0 != null;
			}
			if (set1.contains(communityIndex)) {
				scoreNode1 = comm.getScore(nodes[1], redisConnection);
				commContainsNode1 = scoreNode1 != null;
//				LOGGER.warn("scorenode1: " + scoreNode1);
			}
			
			// if adjacent node is a seed, add 1
			if (comm.isSeed(nodes[0])) {
				redisAsyncConnection.incrbyfloat(i + ":" + nodes[1], 1D);
				redisAsyncConnection.incrbyfloat(DiCeSBolt.COMMUNITY_DEGREES_SUM, 1D);
			}
			// else if adjacent node is a member add estimate of participation /
			// estimate of degree
			else if (commContainsNode0) {
				redisAsyncConnection.incrbyfloat(i + ":" + nodes[1], scoreNode0);
				redisAsyncConnection.incrbyfloat(DiCeSBolt.COMMUNITY_DEGREES_SUM, scoreNode0);
			}
			// if adjacent node is a seed, add 1
			if (comm.isSeed(nodes[1])) {
				redisAsyncConnection.incrbyfloat(i + ":" + nodes[0], 1D);
				redisAsyncConnection.incrbyfloat(DiCeSBolt.COMMUNITY_DEGREES_SUM, 1D);
			}
			// else if adjacent node is a member add estimate of participation /
			// estimate of degree
			else if (commContainsNode1) {
				redisAsyncConnection.incrbyfloat(i + ":" + nodes[0], scoreNode1);
				redisAsyncConnection.incrbyfloat(DiCeSBolt.COMMUNITY_DEGREES_SUM, scoreNode1);
			}
			// if adjacent node is a member add node to community
			try {
				if (commContainsNode0 && !comm.isSeed(nodes[1])) {
					double degree1 = Double.parseDouble(redisConnection.get(nodes[1])) + degreeSum;
					String temp = redisConnection.get(i + ":" + nodes[1]);
					double commDegree1 = temp != null ? Double.parseDouble(temp) + commDegreeSum : 0D + commDegreeSum;
					comm.put(nodes[1], (commDegree1 / degree1), redisAsyncConnection);
					commContainsNode1 = true;
				}
				if (commContainsNode1 && !comm.isSeed(nodes[0])) {
					double degree0 = Double.parseDouble(redisConnection.get(nodes[0])) + degreeSum;
					String temp = redisConnection.get(i + ":" + nodes[0]);
					double commDegree0 = temp != null ? Double.parseDouble(temp) + commDegreeSum : 0D + commDegreeSum;
					comm.put(nodes[0], (commDegree0 / degree0), redisAsyncConnection);
				}	
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}


	public void setSizeDetermination(SizeDetermination sizeDetermination) {
		SIZE_DETERMINATION = sizeDetermination;
	}


	public void setInputEdgeList(String file) {
		GRAPH_FILE = file;
	}


	public void setInputEdgeListDelimiter(String delimiter) {
		 GRAPH_FILE_DELIMITER = delimiter;
	}


	public void setInputGroundTruthCommunities(String file) {
		GROUND_TRUTH_FILE = file;		
	}


	public void setInputGroundTruthCommunitiesDelimiter(String delimiter) {
		GROUND_TRUTH_FILE_DELIMITER = delimiter;
	}


	public void setRedisConnection(String redisConnection) {
		REDIS_CONNECTION = redisConnection;
	}


	public void setRedisPorts(List<Integer> ports) {
		REDIS_PORTS = ports;
	}
	
	public void setBolts(int bolts) {
		BOLTS = bolts;
	}
	
}
