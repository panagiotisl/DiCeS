package gr.uoa.di.madgik.panagiotisl.dices;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableList;

public class DiCeSSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3100917900801046870L;

	private static final Logger LOGGER = Logger.getLogger(DiCeSSpout.class);
	
	private BufferedReader br;

	private String readLine;
	
	protected static final String PRUNE = "PRUNE";
	
	protected static final String END = "EOF";
	
	protected static final String COMMUNITIES = "COMMUNITIES";

	private SpoutOutputCollector _collector;

	private boolean active = true;
	
	private boolean initialized = false;

	RedisAdvancedClusterAsyncCommands<String, String> async;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		RedisClusterClient client = RedisClusterClient.create(DiCeS.REDIS_CONNECTION + ":" + DiCeS.REDIS_PORTS.get(0));
		StatefulRedisClusterConnection<String, String> connection = client.connect();
		RedisAdvancedClusterCommands<String, String> sync = connection.sync();
		async = connection.async();
		
		_collector = collector;
		
		try {
			File f = new File(DiCeS.GRAPH_FILE);
			br = new BufferedReader(new FileReader(f));
		} catch (IOException e) {
			LOGGER.error(DiCeS.GRAPH_FILE, e);
		}
	}

	@Override
	public void nextTuple() {
		if (!initialized) {
			ArrayList<RedisCommunity> redisCommunities = new ArrayList<>();
			try {				
				RedisClusterClient client = RedisClusterClient.create(DiCeS.REDIS_CONNECTION + ":" + DiCeS.REDIS_PORTS.get(0));
				StatefulRedisClusterConnection<String, String> connection = client.connect();
				RedisAdvancedClusterCommands<String, String> sync = connection.sync();
				RedisAdvancedClusterAsyncCommands<String, String> async = connection.async();
				sync.flushall();
				int count = 0;
				BufferedReader gtcFileBR;
				gtcFileBR = new BufferedReader(new FileReader(new File(DiCeS.GROUND_TRUTH_FILE)));
				String commLine;
				while ((commLine = gtcFileBR.readLine()) != null) {
					String[] comm = commLine.trim().split(DiCeS.GROUND_TRUTH_FILE_DELIMITER);
					Set<Integer> randomNumbers = DiCeS.getRandomNumbers(comm.length, DiCeS.NUMBER_OF_SEEDS);

					HashSet<String> set = new HashSet<>();
					for (int number : randomNumbers) {
						set.add(comm[number]);
					}
					redisCommunities.add(new RedisCommunity(count++, sync, set, comm));
				}
				gtcFileBR.close();
				sync.set(COMMUNITIES, Base64.getEncoder().encodeToString(DiCeS.serialize(redisCommunities)));
				LOGGER.info(String.format("%d communities initialized...", redisCommunities.size()));
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
			try {
				_collector.emit(ImmutableList.of(COMMUNITIES, redisCommunities));
			} catch(Exception e) {
				LOGGER.error(e.getMessage(), e);
			} finally {
				initialized = true;
				LOGGER.info(String.format("LogTime: %s", System.currentTimeMillis()));
			}
		}
		if (active) {
			try {
				if ((readLine = br.readLine()) != null) {
					String id = UUID.randomUUID().toString();
					async.setex(id, 200, readLine);
					_collector.emit(new Values(readLine, null), id);
				} else {
					active = false;
					br.close();
					_collector.emit(new Values(END, null));
				}
			} catch (IOException e) {
				LOGGER.error(e);
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("first", "second"));
	}
	
	@Override
	public void fail(Object msgId) {
		try {
			String line = async.get(msgId.toString()).get();
			LOGGER.info("Resending " + line);
			String id = UUID.randomUUID().toString();
			async.setex(id, 200, line);
			_collector.emit(new Values(line, null), id);
		} catch (InterruptedException e) {
			LOGGER.error(e);
		} catch (ExecutionException e) {
			LOGGER.error(e);
		}
	}

}
