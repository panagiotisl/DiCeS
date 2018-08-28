package gr.uoa.di.madgik.panagiotisl.dices;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PruningBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4482496627316555773L;

	private static Logger LOGGER = Logger.getLogger(PruningBolt.class);
	
	private int endCounter = 0;
	
	private int elementsProcessed = 0;

	private List<RedisCommunity> redisCommunities;
	
	private RedisAdvancedClusterAsyncCommands<String, String> async;
	private RedisAdvancedClusterCommands<String, String> sync;
	
	public static double f1score;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		Object message = input.getValue(0);
		
		if (message instanceof String) {
			endCounter++;
			LOGGER.info("END COUNTER " + endCounter);
			if (endCounter == DiCeS.BOLTS) {
				double total = 0D;
				for (RedisCommunity community : redisCommunities) {
					switch (DiCeS.SIZE_DETERMINATION) {
					case GROUND_TRUTH:
						total += DiCeS.getGroundTruthSizeDetermination(community, sync);
						break;
					case DROP_TAIL:
					default:
						total += DiCeS.getDropTailSizeDetermination(community, sync);
						break;

					}
				}
				f1score = total / redisCommunities.size();
				LOGGER.info(String.format("F1 score: %f", total / redisCommunities.size()));
				LOGGER.info(String.format("LogTime: %s", System.currentTimeMillis()));
			}
		} else if (message instanceof List) {
			this.redisCommunities = (List<RedisCommunity>) message;
		}
		else if (message instanceof Integer) {
			elementsProcessed++;
			if (elementsProcessed % DiCeS.WINDOW_SIZE == 0) {
				LOGGER.info("PRUNING " + elementsProcessed);
				for (RedisCommunity community : redisCommunities) {
					community.pruneCommunity(DiCeS.MAX_COMMUNITY_SIZE, sync, async);
				}
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		RedisClusterClient client = RedisClusterClient.create(DiCeS.REDIS_CONNECTION + ":" + DiCeS.REDIS_PORTS.get(0));
		StatefulRedisClusterConnection<String, String> connection = client.connect();
		async = connection.async();
		sync = connection.sync();
	}

}
