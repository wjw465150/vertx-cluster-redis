/*
 * author: @wjw
 * date:   2023年4月3日 下午4:31:36
 * note: 
 */
package io.vertx.core.shareddata;

import org.junit.Ignore;
import org.junit.Test;

import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.MockRedisCluster;

/**
 *
 */
public class RedisClusteredAsyncMapTest extends ClusteredAsyncMapTest {

  private final MockRedisCluster redisClustered = new MockRedisCluster();

  @Override
  protected ClusterManager getClusterManager() {
    return redisClustered.getClusterManager();
  }

  @Override
  protected VertxOptions getOptions() {
    VertxOptions options = super.getOptions();
    //防止调试的时候出现`BlockedThreadChecker`日志信息
    long blockedThreadCheckInterval = 60 * 60 * 1000L;
    options.setBlockedThreadCheckInterval(blockedThreadCheckInterval);

    return options;
  }
  
  public void after() throws Exception {
    super.after();
    redisClustered.stop();
  }

  @Test
  @Override
  public void testMapReplaceIfPresentTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo",
      onSuccess(map -> {
        map.replaceIfPresent("key", "old", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Test
  @Override
  public void testMapReplaceIfPresentTtlWhenNotPresent() {
    getVertx().sharedData().<String, String>getAsyncMap("foo",
      onSuccess(map -> {
        map.replaceIfPresent("key", "old", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Test
  @Override
  public void testMapReplaceTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo",
      onSuccess(map -> {
        map.replace("key", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Test
  @Override
  public void testMapReplaceTtlWithPreviousValue() {
    getVertx().sharedData().<String, String>getAsyncMap("foo",
      onSuccess(map -> {
        map.replace("key", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Test
  public void testStoreAndGetBuffer() {
    getVertx().sharedData().<String, Buffer>getAsyncMap("foo", onSuccess(map -> {
      map.put("test", Buffer.buffer().appendString("Hello"), onSuccess(putResult -> map.get("test", onSuccess(myBuffer -> {
        assertEquals("Hello", myBuffer.toString());
        testComplete();
      }))));
    }));
    await();
  }

  @Ignore
  @Override
  public void testMapPutThenPutTtl() {
    // This test fails upstream, the test is doing:

    // 1. get the async map: foo
    // 2. store the value "molo" under the key "pipo"
    // 3. store the value "mili" under the key "pipo" with TTL 15
    // 4. get the async map: foo
    // N. every 15, check if key "pipo" is NULL <-- THIS NEVER HAPPENS
  }
}
