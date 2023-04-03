/*
 * author: @wjw
 * date:   2023年4月3日 上午10:52:37
 * note: 
 */
package io.vertx.core;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.redis.MockRedisCluster;
import io.vertx.spi.cluster.redis.RedisClusterManager;
import io.vertx.test.core.AsyncTestBase;

public class ProgrammaticRedisClusterManagerTest extends AsyncTestBase {

  private MockRedisCluster redisCluster = new MockRedisCluster();

  private void testProgrammatic(RedisClusterManager mgr, JsonObject config) throws Exception {
    mgr.setConfig(config);
    assertEquals(config, mgr.getConfig());
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    
    options.setBlockedThreadCheckInterval(60 * 60 * 1000L);  //防止调试的时候出现`BlockedThreadChecker`日志信息
    
    Vertx.clusteredVertx(options, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr.getRedissonClient());
      res.result().close(res2 -> {
        assertTrue(res2.succeeded());
        testComplete();
      });
    });
    await();
  }

  @Test
  public void testProgrammaticSetConfig() throws Exception {
    JsonObject config = redisCluster.getDefaultConfig();
    RedisClusterManager mgr = new RedisClusterManager();
    mgr.setConfig(config);
    testProgrammatic(mgr, config);
  }

  @Test
  public void testProgrammaticSetWithConstructor() throws Exception {
    JsonObject config = redisCluster.getDefaultConfig();
    RedisClusterManager mgr = new RedisClusterManager(config);
    testProgrammatic(mgr, config);
  }

  @Test
  public void testCustomRedis() throws Exception {
    JsonObject config = redisCluster.getDefaultConfig();
    RedissonClient redisson = Redisson.create(Config.fromJSON(config.encode()));
    
    RedisClusterManager mgr = new RedisClusterManager(redisson);
    testProgrammatic(mgr, config);
  }

  @Test
  public void testEventBusWhenUsingACustomRedis() throws Exception {
    JsonObject config = redisCluster.getDefaultConfig();
    RedissonClient redisson1 = Redisson.create(Config.fromJSON(config.encode()));
    RedissonClient redisson2 = Redisson.create(Config.fromJSON(config.encode()));

    RedisClusterManager mgr1 = new RedisClusterManager(redisson1);
    RedisClusterManager mgr2 = new RedisClusterManager(redisson2);
    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
    options1.setBlockedThreadCheckInterval(60 * 60 * 1000L);  //防止调试的时候出现`BlockedThreadChecker`日志信息
    options2.setBlockedThreadCheckInterval(60 * 60 * 1000L);  //防止调试的时候出现`BlockedThreadChecker`日志信息

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getRedissonClient());
      res.result().eventBus().consumer("news", message -> {
        System.out.println("consumer news: "+message.body());
        assertNotNull(message);
        assertTrue(message.body().equals("hello"));
        message.reply("hi");
      });
      vertx1.set(res.result());
    });

    assertWaitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getRedissonClient());
      vertx2.set(res.result());
      res.result().eventBus().request("news", "hello", ar -> {
        System.out.println("request news: "+ar.result().body());
        assertTrue(ar.succeeded());
        testComplete();
      });
    });

    await();

    vertx1.get().close(ar -> vertx1.set(null));
    vertx2.get().close(ar -> vertx2.set(null));

    assertTrue(redisson1.isShutdown() == false);
    assertTrue(redisson2.isShutdown() == false);

    assertWaitUntil(() -> vertx1.get() == null && vertx2.get() == null);

    redisson1.shutdown();
    redisson1.shutdown();

  }

  @Test
  public void testSharedDataUsingCustomRedis() throws Exception {
    JsonObject config = redisCluster.getDefaultConfig();
    RedissonClient redisson1 = Redisson.create(Config.fromJSON(config.encode()));
    RedissonClient redisson2 = Redisson.create(Config.fromJSON(config.encode()));

    RedisClusterManager mgr1 = new RedisClusterManager(redisson1);
    RedisClusterManager mgr2 = new RedisClusterManager(redisson2);
    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
    options1.getEventBusOptions().setHost("127.0.0.1");
    options1.setBlockedThreadCheckInterval(60 * 60 * 1000L);  //防止调试的时候出现`BlockedThreadChecker`日志信息
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
    options2.getEventBusOptions().setHost("127.0.0.1");
    options2.setBlockedThreadCheckInterval(60 * 60 * 1000L);  //防止调试的时候出现`BlockedThreadChecker`日志信息

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getRedissonClient());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().put("news", "hello", v -> {
          vertx1.set(res.result());
        });
      });
    });

    assertWaitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getRedissonClient());
      vertx2.set(res.result());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().get("news", r -> {
          assertEquals("hello", r.result());
          testComplete();
        });
      });
    });

    await();

    vertx1.get().close(ar -> vertx1.set(null));
    vertx2.get().close(ar -> vertx2.set(null));

    assertWaitUntil(() -> vertx1.get() == null && vertx2.get() == null);

    assertTrue(redisson1.isShutdown() == false);
    assertTrue(redisson2.isShutdown() == false);

    redisson1.shutdown();
    redisson1.shutdown();
  }

  @Test
  public void testThatExternalCuratorCanBeShutdown() throws Exception {
    // This instance won't be used by vert.x
    JsonObject config = redisCluster.getDefaultConfig();
    RedissonClient redisson1 = Redisson.create(Config.fromJSON(config.encode()));
    String nodeID = UUID.randomUUID().toString();

    RedisClusterManager mgr = new RedisClusterManager(redisson1, nodeID);
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    options.getEventBusOptions().setHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();

    Vertx.clusteredVertx(options, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr.getRedissonClient());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().put("news", "hello", v -> {
          vertx1.set(res.result());
        });
      });
    });

    assertWaitUntil(() -> vertx1.get() != null);
    int size = mgr.getNodes().size();
    assertTrue(size > 0);
    assertTrue(mgr.getNodes().contains(nodeID));

    // Retrieve the value inserted by vert.x
    try {
      RMapCache<String,String> map = redisson1.getMapCache("__vertx:asyncmaps:mymap1");
      String result = map.get("news");
      assertEquals("hello", result);
    } catch (Exception e) {
      e.printStackTrace();
    }
    TimeUnit.SECONDS.sleep(5);
    redisson1.shutdown();

    TimeUnit.SECONDS.sleep(5);
    assertWaitUntil(() -> mgr.getNodes().size() == size - 1);
    vertx1.get().close();
    vertx1.get().close(ar -> vertx1.set(null));

    assertWaitUntil(() -> vertx1.get() == null);
  }

  @Test
  public void testSharedDataUsingCustomCuratorFrameworks() throws Exception {
    JsonObject config = redisCluster.getDefaultConfig();
    RedissonClient dataNode1 =Redisson.create(Config.fromJSON(config.encode()));
    RedissonClient dataNode2 =Redisson.create(Config.fromJSON(config.encode()));

    RedissonClient redisson1 = Redisson.create(Config.fromJSON(config.encode()));
    RedissonClient redisson2 = Redisson.create(Config.fromJSON(config.encode()));

    RedisClusterManager mgr1 = new RedisClusterManager(redisson1);
    RedisClusterManager mgr2 = new RedisClusterManager(redisson2);
    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
    options1.getEventBusOptions().setHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
    options2.getEventBusOptions().setHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getRedissonClient());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().put("news", "hello", v -> {
          vertx1.set(res.result());
        });
      });
    });

    assertWaitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getRedissonClient());
      vertx2.set(res.result());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().get("news", r -> {
          assertEquals("hello", r.result());
          testComplete();
        });
      });
    });

    await();

    vertx1.get().close(ar -> vertx1.set(null));
    vertx2.get().close(ar -> vertx2.set(null));

    assertWaitUntil(() -> vertx1.get() == null && vertx2.get() == null);

    assertTrue(redisson1.isShutdown() == false);
    assertTrue(redisson2.isShutdown() == false);

    redisson1.shutdown();
    redisson1.shutdown();

    assertTrue(dataNode1.isShutdown() == false);
    assertTrue(dataNode2.isShutdown() == false);

    dataNode1.shutdown();
    dataNode2.shutdown();
  }

}
