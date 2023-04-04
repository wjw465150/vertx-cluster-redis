package io.vertx.ext.web.sstore;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.MockRedisCluster;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class RedisClusteredSessionHandlerTest extends ClusteredSessionHandlerTest {

  private MockRedisCluster redisCluster = new MockRedisCluster();

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    redisCluster.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return redisCluster.getClusterManager();
  }

  @Ignore @Test
  public void testSessionExpires() {

  }
}
