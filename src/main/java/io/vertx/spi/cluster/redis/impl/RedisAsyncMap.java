/*
 * author: @wjw
 * date:   2023年4月4日 下午4:21:40
 * note: 
 */
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
import org.redisson.api.RedissonClient;
import org.redisson.codec.TypedJsonJacksonCodec;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;

public class RedisAsyncMap<K, V> implements AsyncMap<K, V> {

  private final Vertx             vertx;
  private final RMapCache<byte[], byte[]> map;

  public RedisAsyncMap(Vertx vertx, RedissonClient redisson, String mapName) {
    this.vertx = vertx;
    this.map = redisson.getMapCache(mapName, new TypedJsonJacksonCodec(byte[].class, byte[].class));
  }

  private void noopRun() {
    //
  }

  @Override
  public Future<V> get(K k) {
    Context context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.getAsync(ConversionUtils.asByte(k)).thenApplyAsync(ConversionUtils::asObject), context);
  }

  @Override
  public Future<Void> put(K k, V v) {
    Context context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putAsync(ConversionUtils.asByte(k), ConversionUtils.asByte(v)).thenRun(this::noopRun), context);
  }

  @Override
  public Future<V> putIfAbsent(K k, V v) {
    Context context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putIfAbsentAsync(ConversionUtils.asByte(k), ConversionUtils.asByte(v)).thenApplyAsync(ConversionUtils::asObject), context);
  }

  @Override
  public Future<Void> put(K k, V v, long ttl) {
    Context context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putAsync(ConversionUtils.asByte(k), ConversionUtils.asByte(v), ttl, TimeUnit.MILLISECONDS).thenRun(this::noopRun), context);
  }

  @Override
  public Future<V> putIfAbsent(K k, V v, long ttl) {
    Context context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.putIfAbsentAsync(ConversionUtils.asByte(k), ConversionUtils.asByte(v), ttl, TimeUnit.MILLISECONDS).thenApplyAsync(ConversionUtils::asObject), context);
  }

  @Override
  public Future<V> remove(K k) {
    Context context = vertx.getOrCreateContext();
    return Future.fromCompletionStage(map.removeAsync(ConversionUtils.asByte(k)).thenApplyAsync(ConversionUtils::asObject), context);
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    return vertx.executeBlocking(fut -> fut.complete(map.remove(ConversionUtils.asByte(k), ConversionUtils.asByte(v))), false);
  }

  @Override
  public Future<V> replace(K k, V v) {
    return vertx.executeBlocking(fut -> fut.complete(ConversionUtils.asObject(map.replace(ConversionUtils.asByte(k), ConversionUtils.asByte(v)))), false);
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
    return vertx.executeBlocking(fut -> fut.complete(map.replace(ConversionUtils.asByte(k), ConversionUtils.asByte(oldValue), ConversionUtils.asByte(newValue))), false);
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
      for (byte[] k : map.keySet()) {
        set.add(ConversionUtils.asObject(k));
      }
      fut.complete(set);
    }, false);
  }

  @Override
  public Future<List<V>> values() {
    return vertx.executeBlocking(fut -> {
      List<V> list = new ArrayList<>();
      for (byte[] v : map.values()) {
        list.add(ConversionUtils.asObject(v));
      }
      fut.complete(list);
    }, false);
  }

  @Override
  public Future<Map<K, V>> entries() {
    return vertx.executeBlocking(fut -> {
      Map<K, V> result = new HashMap<>();
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
        byte[] k = entry.getKey();
        byte[] v = entry.getValue();
        result.put(ConversionUtils.asObject(k), ConversionUtils.asObject(v));
      }
      fut.complete(result);
    }, false);
  }

}
