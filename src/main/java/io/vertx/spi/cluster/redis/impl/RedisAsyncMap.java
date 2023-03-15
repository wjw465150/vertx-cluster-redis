/*
 * Copyright (c) 2019 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.spi.cluster.redis.impl;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.client.protocol.convertor.LongReplayConvertor;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;

/**
 *
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class RedisAsyncMap<K, V> implements AsyncMap<K, V> { // extends MapTTL<K, V>
  // private static final Logger log = LoggerFactory.getLogger(RedisAsyncMap.class);

  protected final RedisStrictCommand<Long> ZSCORE_LONG = new RedisStrictCommand<Long>("ZSCORE",
      new LongReplayConvertor()); // RedisCommands.ZSCORE

  protected final Vertx           vertx;
  protected final RedissonClient  redisson;
  protected final RMapCache<K, V> map;
  protected final String          name;

  public RedisAsyncMap(Vertx vertx, RedissonClient redisson, String name, Codec codec) {
    // super(vertx, redisson, name);
    Objects.requireNonNull(redisson, "redisson");
    Objects.requireNonNull(name, "name");
    this.vertx = vertx;
    this.redisson = redisson;
    this.name = name;
    this.map = createRMapCache(this.redisson, this.name, codec);
  }

  /**
   * Here you can customize(override method) a "Codec"
   * 
   * @see org.redisson.codec.JsonJacksonCodec
   * @see org.redisson.codec.FstCodec
   */
  protected RMapCache<K, V> createRMapCache(RedissonClient redisson, String name, Codec codec) {
    if (codec == null) {
      return redisson.getMapCache(name); // redisson.getMapCache(name, new RedisMapCodec());
    } else {
      return redisson.getMapCache(name, codec);
    }
  }

  @Override
  public Future<V> get(K k) {
    Promise<V> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.getAsync(k).whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  public Future<Void> put(K k, V v) {
    Promise<Void> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.fastPutAsync(k, v).whenComplete((b, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete();
      }
    });
    return promise.future();
  }

  @Override
  public Future<Void> put(K k, V v, long ttl) {
    Promise<Void> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.fastPutAsync(k, v, ttl, TimeUnit.MILLISECONDS).whenComplete((b, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete();
      }
    });
    return promise.future();
  }

  @Override
  /**
   * @return Previous value if key already exists else null
   */
  public Future<V> putIfAbsent(K k, V v) {
    Promise<V> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.putIfAbsentAsync(k, v).whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  /**
   * @return Previous value if key already exists else null
   */
  public Future<V> putIfAbsent(K k, V v, long ttl) {
    Promise<V> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.putIfAbsentAsync(k, v, ttl, TimeUnit.MILLISECONDS).whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  public Future<V> remove(K k) {
    Promise<V> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.removeAsync(k).whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    Promise<Boolean> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.removeAsync(k, v).whenComplete((b, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete();
      }
    });
    return promise.future();
  }

  /**
   * @return previous (old) value
   */
  @Override
  public Future<V> replace(K k, V v) {
    Promise<V> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.replaceAsync(k, v).whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
    Promise<Boolean> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.replaceAsync(k, oldValue, newValue).whenComplete((b, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete();
      }
    });
    return promise.future();
  }

  @Override
  public Future<Void> clear() {
    Promise<Void> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.deleteAsync().whenComplete((b, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete();
      }
    });
    return promise.future();
  }

  @Override
  public Future<Integer> size() {
    Promise<Integer> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.sizeAsync().whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  public Future<Set<K>> keys() {
    Promise<Set<K>> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.readAllKeySetAsync().whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  public Future<List<V>> values() {
    Promise<List<V>> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.readAllValuesAsync().whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete((List<V>) rv);
      }
    });
    return promise.future();
  }

  @Override
  public Future<Map<K, V>> entries() {
    Promise<Map<K, V>> promise = ((VertxInternal) vertx).getOrCreateContext().promise();
    map.readAllMapAsync().whenComplete((rv, e) -> {
      if (e != null) {
        promise.tryFail(e);
      } else {
        promise.tryComplete(rv);
      }
    });
    return promise.future();
  }

  @Override
  public String toString() {
    return "RedisAsyncMap [name=" + name + "]";
  }

}
