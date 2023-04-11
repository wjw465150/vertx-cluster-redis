/*
 * author: @wjw
 * date:   2023年4月4日 下午4:22:03
 * note: 
 */
package io.vertx.spi.cluster.redis.impl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMultimapCache;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;

public class SubsMapHelper {
  private static final Logger log = LoggerFactory.getLogger(SubsMapHelper.class);

  private final RedissonClient                               redisson;
  private final RMultimapCache<String, RegistrationInfo>     subsCache;
  private final VertxInternal                                vertx;
  private final NodeSelector                                 nodeSelector;
  private final String                                       nodeId;
  private final ConcurrentMap<String, Set<RegistrationInfo>> ownSubs = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<RegistrationInfo>> localSubs = new ConcurrentHashMap<>();

  private static final String VERTX_SUBS_NAME = "__vertx:subs";

  public SubsMapHelper(VertxInternal vertx, RedissonClient redisson, NodeSelector nodeSelector, String nodeId) {
    this.vertx = vertx;
    this.redisson = redisson;
    this.subsCache = redisson.getSetMultimapCache(VERTX_SUBS_NAME, JsonJacksonCodec.INSTANCE);

    this.nodeSelector = nodeSelector;
    this.nodeId = nodeId;
  }

  public void updateSubsEntryExpiration(long ttl, TimeUnit ttlUnit) {
    Iterator<String> subsKeySetIterator = ownSubs.keySet().iterator();
    while (subsKeySetIterator.hasNext()) {
      subsCache.expireKey(subsKeySetIterator.next(), ttl, ttlUnit);
    }
  }

  public void close() {
    //
  }

  public void put(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    if (registrationInfo.localOnly()) {
      localSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));
      fireRegistrationUpdateEvent(address);
      promise.complete();
    } else {
      try {
        ownSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));
        subsCache.put(address, registrationInfo);
        promise.complete();
      } catch (Exception e) {
        log.error(String.format("create subs address %s failed.", address), e);
        promise.fail(e);
      }
    }
  }

  private Set<RegistrationInfo> addToSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> currSet) {
    Set<RegistrationInfo> res = currSet != null ? currSet : Collections.synchronizedSet(new LinkedHashSet<>());
    res.add(registrationInfo);
    return res;
  }

  public List<RegistrationInfo> get(String address) {
    List<RegistrationInfo>       list;
    Collection<RegistrationInfo> remote = subsCache.get(address);
    int                          size;
    size = remote.size();
    Set<RegistrationInfo> local = localSubs.get(address);
    if (local != null) {
      synchronized (local) {
        size += local.size();
        if (size == 0) {
          return Collections.emptyList();
        }
        list = new ArrayList<>(size);
        list.addAll(local);
      }
    } else if (size == 0) {
      return Collections.emptyList();
    } else {
      list = new ArrayList<>(size);
    }
    for (RegistrationInfo registrationInfo : remote) {
      list.add(registrationInfo);
    }
    return list;
  }

  public void remove(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    try {
      if (registrationInfo.localOnly()) {
        localSubs.computeIfPresent(address, (add, curr) -> removeFromSet(registrationInfo, curr));
        fireRegistrationUpdateEvent(address);
        promise.complete();
      } else {
        ownSubs.computeIfPresent(address, (add, curr) -> removeFromSet(registrationInfo, curr));
        //删除指令来的早了,节点还不存在,这时候重试几次!
        //@wjw_comment: 为何不用Watcher来监听?因为可能在创建Watcher的时候节点已经创建了,就监听不到了!
        if (!subsCache.remove(address, registrationInfo)) {
          java.util.concurrent.atomic.AtomicInteger retryCount = new java.util.concurrent.atomic.AtomicInteger(0);
          vertx.setPeriodic(100, 100, timerID -> {
            try {
              log.warn(MessageFormat.format("要删除的Redis节点不存在:{0}, 重试第:{1}次!", address, retryCount.incrementAndGet()));
              if (subsCache.remove(address, registrationInfo)) {
                vertx.cancelTimer(timerID);
                log.warn(MessageFormat.format("重试第:{0}次后,成功删除Redis节点:{1}", retryCount.get(), address));
                promise.complete();
                return;
              }

              if (retryCount.get() > 10) {
                vertx.cancelTimer(timerID);
                String errMessage = MessageFormat.format("重试{0}次后,要删除的Redis节点还不存在:{1}", retryCount.get(), address);
                log.warn(errMessage);
                throw new IllegalStateException(errMessage);
              }
            } catch (Exception e) {
              log.error(e.getMessage(), e);
            }
          });

          return;
        }

        promise.complete();
      }
    } catch (Exception e) {
      log.error(String.format("remove subs address %s failed.", address), e);
      promise.fail(e);
    }
  }

  private Set<RegistrationInfo> removeFromSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> curr) {
    curr.remove(registrationInfo);
    return curr.isEmpty() ? null : curr;
  }

  private void fireRegistrationUpdateEvent(String address) {
    nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, get(address)));
  }
  
  public void syncOwnSubs2Remote() {
    log.info(String.format("vertx node %s have reconnected to Redis", nodeId));
    vertx.runOnContext(aVoid -> {
      List<Future> futures = new ArrayList<>();
      for (Map.Entry<String, Set<RegistrationInfo>> entry : ownSubs.entrySet()) {
        for (RegistrationInfo registrationInfo : entry.getValue()) {
          Promise<Void> promise = Promise.promise();
          put(entry.getKey(), registrationInfo, promise);
          futures.add(promise.future());
        }
      }
      CompositeFuture.all(futures).onComplete(ar -> {
        if (ar.failed()) {
          log.error("recover node subs information failed.", ar.cause());
        } else {
          log.info("recover node subs success.");
        }
      });
    });
  }

}
