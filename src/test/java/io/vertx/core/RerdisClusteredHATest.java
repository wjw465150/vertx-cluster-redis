/*
 * author: @wjw
 * date:   2023年4月2日 下午5:05:54
 * note: 
 */
package io.vertx.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.MockRedisCluster;

public class RerdisClusteredHATest extends HATest {

  private MockRedisCluster redisClustered = new MockRedisCluster();

  public void after() throws Exception {
    super.after();
    redisClustered.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return redisClustered.getClusterManager();
  }
}
