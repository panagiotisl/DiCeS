package gr.uoa.di.madgik.panagiotisl.dices;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class DiCeSBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(DiCeSBolt.class);
	
	private static final long serialVersionUID = 2582648360999730523L;

	private static final Values ELEMENT_VALUE = new Values(Integer.valueOf(1));
	
	private static final Values END_VALUE = new Values(DiCeSSpout.END);
	
	public static final String DEGREES_SUM = "DEGREES_SUM";
	
	public static final String COMMUNITY_DEGREES_SUM = "COMMUNITY_DEGREES_SUM";
	
	private transient RedisAdvancedClusterAsyncCommands<String, String> async;
	private transient RedisAdvancedClusterCommands<String, String> sync;
	private List<RedisCommunity> redisCommunities;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			
			if (input.getValue(1) != null) {
				this.redisCommunities = (List<RedisCommunity>) input.getValue(1);
				collector.emit(new Values(redisCommunities));
				return;
			}
			
			if (!input.getString(0).equals(DiCeSSpout.END)) {
				String[] nodes = input.getString(0).split(DiCeS.GRAPH_FILE_DELIMITER);
				// do not allow self loops
				if (nodes[0].equals(nodes[1]))
					return;
				DiCeS.addToDegreeRedis(nodes, async);
				DiCeS.addToCommRedisPRV(nodes, sync, async, redisCommunities);
				collector.emit(ELEMENT_VALUE);
			} else {
				LOGGER.info("Received end message...");
				collector.emit(END_VALUE);
			}
		} catch (Exception e) {
			LOGGER.error("EXC: ", e);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("command"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		RedisClusterClient client = RedisClusterClient.create(DiCeS.REDIS_CONNECTION + ":" + DiCeS.REDIS_PORTS.get(0));
		StatefulRedisClusterConnection<String, String> connection = client.connect();
		async = connection.async();
		sync = connection.sync();

		redisCommunities = new ArrayList<>();
		
		LOGGER.info("Bolt initialized...");
	}

}
