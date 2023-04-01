package io.vertx.spi.cluster.redis.impl;

import org.redisson.api.RAtomicLong;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.Counter;

public class RedisCounter implements Counter {
  private final VertxInternal vertx;

  private final RAtomicLong counter;

  public RedisCounter(VertxInternal vertx, RAtomicLong counter) {
    this.vertx = vertx;
    this.counter = counter;
  }

  @Override
  public Future<Long> get() {
    Promise<Long> promise = Promise.promise();
    Context       context = vertx.getOrCreateContext();
    counter.getAsync().whenComplete((rv, e) -> context.runOnContext(vd -> {
      if (e != null) {
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    })
    );

    return promise.future();
  }

  @Override
  public Future<Long> incrementAndGet() {
    Promise<Long> promise = Promise.promise();
    Context       context = vertx.getOrCreateContext();
    counter.incrementAndGetAsync().whenComplete((rv, e) -> context.runOnContext(vd -> {
      if (e != null) {
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    })
    );

    return promise.future();
  }

  @Override
  public Future<Long> getAndIncrement() {
    Promise<Long> promise = Promise.promise();
    Context       context = vertx.getOrCreateContext();
    counter.getAndIncrementAsync().whenComplete((rv, e) -> context.runOnContext(vd -> {
      if (e != null) {
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    })
    );

    return promise.future();
  }

  @Override
  public Future<Long> decrementAndGet() {
    Promise<Long> promise = Promise.promise();
    Context       context = vertx.getOrCreateContext();
    counter.decrementAndGetAsync().whenComplete((rv, e) -> context.runOnContext(vd -> {
      if (e != null) {
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    })
    );

    return promise.future();
  }

  @Override
  public Future<Long> addAndGet(long value) {
    Promise<Long> promise = Promise.promise();
    Context       context = vertx.getOrCreateContext();
    counter.addAndGetAsync(value).whenComplete((rv, e) -> context.runOnContext(vd -> {
      if (e != null) {
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    })
    );

    return promise.future();
  }

  @Override
  public Future<Long> getAndAdd(long value) {
    Promise<Long> promise = Promise.promise();
    Context       context = vertx.getOrCreateContext();
    counter.getAndAddAsync(value).whenComplete((rv, e) -> context.runOnContext(vd -> {
      if (e != null) {
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    })
    );

    return promise.future();
  }

  @Override
  public Future<Boolean> compareAndSet(long expected, long value) {
    Promise<Boolean> promise = Promise.promise();
    Context          context = vertx.getOrCreateContext();
    counter.compareAndSetAsync(expected, value).whenComplete((rv, e) -> context.runOnContext(vd -> {
      if (e != null) {
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    })
    );

    return promise.future();
  }
}
