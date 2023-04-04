/*
 * author: @wjw
 * date:   2023年4月4日 下午4:23:26
 * note: 
 */
package io.vertx.spi.cluster.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.redis.impl.ConfigUtil;
import io.vertx.spi.cluster.redis.impl.RedisSyncMap;

public class RedisSyncMapTest {
  static RedissonClient redisson;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    JsonObject conf   = ConfigUtil.loadConfig("classpath:redis.json");
    Config     config = Config.fromJSON(conf.encode());
    redisson = Redisson.create(config);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    redisson.shutdown();
  }

  @Test
  public void testSyncMapOperation() {
    String k = "myKey";
    String v = "myValue";

    RedisSyncMap<String, String> syncMap = new RedisSyncMap<>(redisson,"__vertx:syncmaps:" + "mapTest");

    syncMap.put(k, v);
    assertFalse(syncMap.isEmpty());

    assertEquals(syncMap.get(k), v);

    assertTrue(syncMap.size() > 0);
    assertTrue(syncMap.containsKey(k));
    assertTrue(syncMap.containsValue(v));

    assertTrue(syncMap.keySet().contains(k));
    assertTrue(syncMap.values().contains(v));

    syncMap.entrySet().forEach(entry -> {
      assertEquals(k, entry.getKey());
      assertEquals(v, entry.getValue());
    });

    String value = syncMap.remove(k);
    assertEquals(value, v);
    assertNull(syncMap.get(k));

    syncMap.clear();
    assertTrue(syncMap.isEmpty());
  }

}
