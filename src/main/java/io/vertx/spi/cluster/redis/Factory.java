package io.vertx.spi.cluster.redis;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RedissonClient;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.impl.DefaultFactory;

public interface Factory {

  <K, V> AsyncMap<K, V> createAsyncMap(Vertx vertx, RedissonClient redisson, String name);

  <K, V> Map<K, V> createMap(Vertx vertx, RedissonClient redisson, String name);

  interface ExpirableAsync<K> {

    /**
     * Remaining time to live
     * 
     * @return TTL in milliseconds
     */
    void getTTL(K k, Handler<AsyncResult<Long>> resultHandler);

    /**
     * Refresh TTL if present. Only update elements that already exist. Never add elements.
     * 
     * @return The number of elements added to the sorted sets, not including elements already existing for which the
     *         score was updated
     */
    void refreshTTLIfPresent(K k, long timeToLive, TimeUnit timeUnit, Handler<AsyncResult<Long>> resultHandler);

  }

  public static Factory createDefaultFactory() {
    return new DefaultFactory();
  }

}
