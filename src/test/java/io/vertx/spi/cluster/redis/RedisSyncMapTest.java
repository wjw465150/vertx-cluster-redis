package io.vertx.spi.cluster.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.redis.impl.ConfigUtil;
import io.vertx.spi.cluster.redis.impl.RedisSyncMap;

public class RedisSyncMapTest {
  static RedissonClient redisson;

  @BeforeAll
  static void init() throws IOException {
    JsonObject conf   = ConfigUtil.loadConfig("classpath:redis.json");
    Config     config = Config.fromJSON(conf.encode());
    redisson = Redisson.create(config);
  }

  @AfterAll
  static void destory() {
    redisson.shutdown();
  }

  @Test
  public void syncMapOperation() throws Exception {

    String k = "myKey";
    String v = "myValue";

    RedisSyncMap<String, String> syncMap = new RedisSyncMap<>(redisson.getMapCache("__vertx:syncmaps:" + "mapTest",JsonJacksonCodec.INSTANCE));

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
