/*
 * author: @wjw
 * date:   2023年4月4日 下午4:23:19
 * note: 
 */
package io.vertx.spi.cluster.redis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.impl.ConfigUtil;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMap;
import io.vertx.test.core.VertxTestBase;

public class RedisAsyncMapTest extends VertxTestBase {
  static RedissonClient redisson;

  @Override
  protected ClusterManager getClusterManager() {
    try {
      JsonObject conf   = ConfigUtil.loadConfig("classpath:redis.json");
      Config     config = Config.fromJSON(conf.encode());
      redisson = Redisson.create(config);

      RedisClusterManager zookeeperClusterManager = new RedisClusterManager(redisson);
      return zookeeperClusterManager;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    redisson.shutdown();
  }

  @Override
  protected VertxOptions getOptions() {
    VertxOptions options = super.getOptions();
    //防止调试的时候出现`BlockedThreadChecker`日志信息
    long blockedThreadCheckInterval = 60 * 60 * 1000L;
    options.setBlockedThreadCheckInterval(blockedThreadCheckInterval);

    return options;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    startNodes(1);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testSyncMapOperation() {
    String k = "myKey";
    String v = "myValue";

    RedisAsyncMap<String, String> aSyncMap = new RedisAsyncMap<>(vertx, redisson, "__vertx:asyncmaps:" + "mapTest");

    aSyncMap.size().compose(size -> {
      assertFalse(size == 1);
      return Future.succeededFuture();
    }).compose(obj -> {
      return aSyncMap.put(k, v);
    }).compose(vVoid -> {
      return aSyncMap.get(k);
    }).compose(value -> {
      assertEquals(value, v);
      return aSyncMap.keys();
    }).compose(keys -> {
      assertTrue(keys.contains(k));
      return aSyncMap.values();
    }).compose(values -> {
      assertTrue(values.contains(v));
      return aSyncMap.entries();
    }).compose(map -> {
      map.entrySet().forEach(entry -> {
        assertEquals(k, entry.getKey());
        assertEquals(v, entry.getValue());
      });
      return aSyncMap.remove(k);
    }).compose(value -> {
      assertEquals(value, v);
      return aSyncMap.get(k);
    }).compose(value -> {
      assertNull(value);
      return Future.succeededFuture();
    }).compose(vVoid -> {
      return aSyncMap.clear();
    }).compose(vVoid -> {
      return aSyncMap.size();
    }).onSuccess(size -> {
      assertTrue(size == 0);
      this.complete();
    });

    await(100, TimeUnit.SECONDS);
  }

}
