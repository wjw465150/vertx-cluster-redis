package io.vertx.spi.cluster.redis.impl;

import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.cluster.RegistrationInfo;

import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class SubsOpSerializer {

  private final VertxInternal vertx;
  private final TaskQueue taskQueue;

  private SubsOpSerializer(VertxInternal vertx) {
    this.vertx = vertx;
    taskQueue = new TaskQueue();
  }

  public static SubsOpSerializer get(ContextInternal context) {
    ConcurrentMap<Object, Object> contextData = context.contextData();
    SubsOpSerializer instance = (SubsOpSerializer) contextData.get(SubsOpSerializer.class);
    if (instance == null) {
      SubsOpSerializer candidate = new SubsOpSerializer(context.owner());
      SubsOpSerializer previous = (SubsOpSerializer) contextData.putIfAbsent(SubsOpSerializer.class, candidate);
      instance = previous == null ? candidate : previous;
    }
    return instance;
  }

  public void execute(BiConsumer<String, RegistrationInfo> op, String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    taskQueue.execute(() -> {
      try {
        op.accept(address, registrationInfo);
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      }
    }, vertx.getWorkerPool().executor());
  }
}
