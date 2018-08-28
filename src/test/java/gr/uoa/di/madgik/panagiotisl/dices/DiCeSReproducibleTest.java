package gr.uoa.di.madgik.panagiotisl.dices;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.ishiis.redis.unit.RedisCluster;

import org.apache.log4j.Logger;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

public class DiCeSReproducibleTest {

	private static Logger LOGGER = Logger.getLogger(DiCeSReproducibleTest.class);
	
	private DiCeS dices;
	
	private RedisCluster cluster;
	
	@Before
	public void init() {
		
		cluster = new RedisCluster(7379, 7380, 7381);
		LOGGER.info("Starting redis cluster...");
		cluster.start();
		
		dices = new DiCeS();
		dices.setBolts(4);
		dices.setSizeDetermination(DiCeS.SizeDetermination.DROP_TAIL);
		dices.setInputEdgeList(Resources.getResource("amazon.txt").getFile());
		dices.setInputEdgeListDelimiter(" ");
		dices.setInputGroundTruthCommunities(Resources.getResource("amazonGTC.txt").getFile());
		dices.setInputGroundTruthCommunitiesDelimiter(" ");
		dices.setRedisConnection("redis://127.0.0.1");
		dices.setRedisPorts(ImmutableList.of(7379, 7380, 7381));
	}
	
	@Test
	public void shouldAchieveAppropriateF1ScoreForAmazonNetwork() throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {

		long startTime = System.nanoTime();
		dices.execute(null);
		
		while (PruningBolt.f1score <= 0) {
			TimeUnit.MILLISECONDS.sleep(100);
		}
		long estimatedTime = System.nanoTime() - startTime;
		
		// F1 score should be around 0.81
		assertEquals(0.81, PruningBolt.f1score, 0.03);
		
		// divide by number of edges and convert to microseconds
		LOGGER.info("Time per edge: " + estimatedTime / (925_872 * 1_000)  + " microseconds");  
		
	}
	
	@After
	public void cleanUp() throws InterruptedException {
		// re-initialize the f1score variable that acts as a flag for termination
		PruningBolt.f1score = 0;
		
		// stop the cluster
		LOGGER.info("Stopping redis cluster...");
		cluster.stop();
	}
	
}
