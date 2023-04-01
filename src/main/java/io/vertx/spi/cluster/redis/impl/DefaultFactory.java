package io.vertx.spi.cluster.redis.impl;

import java.util.Map;

import org.redisson.api.RedissonClient;

import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.spi.cluster.redis.Factory;


public class DefaultFactory implements Factory {
  @Override
  public <K, V> AsyncMap<K, V> createAsyncMap(Vertx vertx, RedissonClient redisson, String name) {
    return new RedisAsyncMap<>((VertxInternal) vertx, redisson.getMapCache(name));
  }

  @Override
  public <K, V> Map<K, V> createMap(Vertx vertx, RedissonClient redisson, String name) {
    return new RedisMap<>(vertx, redisson, name);
  }


}
