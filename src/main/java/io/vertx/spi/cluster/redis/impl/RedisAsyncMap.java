package io.vertx.spi.cluster.redis.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMapCache;

import io.vertx.core.Future;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;

public class RedisAsyncMap<K, V> implements AsyncMap<K, V> {

  private final VertxInternal   vertx;
  private final RMapCache<K, V> map;

  public RedisAsyncMap(VertxInternal vertx, RMapCache<K, V> map) {
    this.vertx = vertx;
    this.map = map;
  }

  private void noopRun() {
    //
  }

  @Override
  public Future<V> get(K k) {
    ContextInternal context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.getAsync(k), context);
  }

  @Override
  public Future<Void> put(K k, V v) {
    ContextInternal context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putAsync(k, v).thenRun(this::noopRun), context);
  }

  @Override
  public Future<V> putIfAbsent(K k, V v) {
    ContextInternal context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putIfAbsentAsync(k, v), context);
  }

  @Override
  public Future<Void> put(K k, V v, long ttl) {
    ContextInternal context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putAsync(k, v, ttl, TimeUnit.MILLISECONDS).thenRun(this::noopRun), context);
  }

  @Override
  public Future<V> putIfAbsent(K k, V v, long ttl) {
    ContextInternal context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putIfAbsentAsync(k, v, ttl, TimeUnit.MILLISECONDS), context);
  }

  @Override
  public Future<V> remove(K k) {
    ContextInternal context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.removeAsync(k), context);
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    return vertx.executeBlocking(fut -> fut.complete(map.remove(k, v)), false);
  }

  @Override
  public Future<V> replace(K k, V v) {
    return vertx.executeBlocking(fut -> fut.complete(map.replace(k, v)), false);
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
    return vertx.executeBlocking(fut -> fut.complete(map.replace(k, oldValue, newValue)), false);
  }

  @Override
  public Future<Void> clear() {
    return vertx.executeBlocking(fut -> {
      map.clear();
      fut.complete();
    }, false);
  }

  @Override
  public Future<Integer> size() {
    return vertx.executeBlocking(fut -> fut.complete(map.size()), false);
  }

  @Override
  public Future<Set<K>> keys() {
    return vertx.executeBlocking(fut -> {
      Set<K> set = new HashSet<>();
      for (K k : map.keySet()) {
        set.add(k);
      }
      fut.complete(set);
    }, false);
  }

  @Override
  public Future<List<V>> values() {
    return vertx.executeBlocking(fut -> {
      List<V> list = new ArrayList<>();
      for (V v : map.values()) {
        list.add(v);
      }
      fut.complete(list);
    }, false);
  }

  @Override
  public Future<Map<K, V>> entries() {
    return vertx.executeBlocking(fut -> {
      Map<K, V> result = new HashMap<>();
      for (Entry<K, V> entry : map.entrySet()) {
        K k = entry.getKey();
        V v = entry.getValue();
        result.put(k, v);
      }
      fut.complete(result);
    }, false);
  }
}
