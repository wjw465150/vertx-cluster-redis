/*
 * author: @wjw
 * date:   2023年4月2日 下午5:06:04
 * note: 
 */
package io.vertx.spi.cluster.redis;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.impl.ConfigUtil;

/**
 * Created by Stream.Liu
 */
public class MockRedisCluster {
  private final List<RedisClusterManager> clusterManagers = new ArrayList<>();

  public MockRedisCluster() {
  }

  public JsonObject getDefaultConfig() {
    JsonObject config = ConfigUtil.loadConfig("classpath:redis.json");
    return config;
  }

  public void stop() {
    try {
      clusterManagers.clear();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public ClusterManager getClusterManager() {
    RedisClusterManager zookeeperClusterManager = new RedisClusterManager(getDefaultConfig());
    clusterManagers.add(zookeeperClusterManager);
    return zookeeperClusterManager;
  }
}
