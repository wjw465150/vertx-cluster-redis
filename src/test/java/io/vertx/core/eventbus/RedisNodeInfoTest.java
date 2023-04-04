package io.vertx.core.eventbus;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.MockRedisCluster;

public class RedisNodeInfoTest extends NodeInfoTest {

  private MockRedisCluster redisCluster = new MockRedisCluster();

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    redisCluster.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return redisCluster.getClusterManager();
  }
}
