package io.vertx.core.eventbus;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.MockRedisCluster;

public class ZKNodeInfoTest extends NodeInfoTest {

  private MockRedisCluster zkClustered = new MockRedisCluster();

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    zkClustered.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }
}
