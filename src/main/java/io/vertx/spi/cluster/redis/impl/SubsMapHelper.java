/*
 * author: @wjw
 * date:   2023年4月4日 下午4:22:03
 * note: 
 */
package io.vertx.spi.cluster.redis.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.redisson.codec.JsonJacksonCodec;

import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;

public class SubsMapHelper
    implements EntryCreatedListener<String, RegistrationInfo>, EntryUpdatedListener<String, RegistrationInfo>, EntryExpiredListener<String, RegistrationInfo>, EntryRemovedListener<String, RegistrationInfo> {
  private static final Logger log = LoggerFactory.getLogger(SubsMapHelper.class);

  private final RedissonClient                               redisson;
  private final RMapCache<String, Set<RegistrationInfo>>     subsCache;
  private final VertxInternal                                vertx;
  private final NodeSelector                                 nodeSelector;
  private final String                                       nodeId;
  private final ConcurrentMap<String, Set<RegistrationInfo>> localSubs = new ConcurrentHashMap<>();
  private final ReadWriteLock                                republishLock;

  private static final String VERTX_SUBS_NAME  = "__vertx:subs";
  private static final String VERTX_SUBS_LOCKS = "__vertx:subs_locks";

  public SubsMapHelper(VertxInternal vertx, RedissonClient redisson, NodeSelector nodeSelector, String nodeId) {
    this.vertx = vertx;
    this.redisson = redisson;
    this.subsCache = redisson.getMapCache(VERTX_SUBS_NAME, JsonJacksonCodec.INSTANCE);
    this.subsCache.addListener(this);
    this.republishLock = redisson.getReadWriteLock(VERTX_SUBS_LOCKS);

    this.nodeSelector = nodeSelector;
    this.nodeId = nodeId;

  }

  public void updateSubsEntryExpiration(long ttl, TimeUnit ttlUnit) {
    Set<String> subsKeySet = subsCache.keySet();
    if (!subsKeySet.isEmpty()) {
      subsKeySet.stream().forEach(key -> {
        try {
          subsCache.updateEntryExpiration(key, ttl, ttlUnit, 0, TimeUnit.SECONDS);
        } catch (Exception e) {
          //e.printStackTrace();
        }
      });
    }
  }

  public void close() {
    subsCache.destroy();
  }

  public void put(String address, RegistrationInfo registrationInfo) {
    Lock writeLock = republishLock.writeLock();
    writeLock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));
        fireRegistrationUpdateEvent(address);
      } else {
        try {
          Set<RegistrationInfo> remoteRegistrationInfoSet = subsCache.get(address);
          if (remoteRegistrationInfoSet == null) {
            remoteRegistrationInfoSet = new HashSet<>();
          }
          remoteRegistrationInfoSet.add(registrationInfo);
          subsCache.fastPut(address, remoteRegistrationInfoSet, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
          log.error(String.format("create subs address %s failed.", address), e);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private Set<RegistrationInfo> addToSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> currSet) {
    Set<RegistrationInfo> res = currSet != null ? currSet : Collections.synchronizedSet(new LinkedHashSet<>());
    res.add(registrationInfo);
    return res;
  }

  public List<RegistrationInfo> get(String address) {
    Lock readLock = republishLock.readLock();
    readLock.lock();
    try {
      Set<RegistrationInfo> remote = subsCache.get(address);
      if (remote == null) {
        remote = new HashSet<>();
      }

      List<RegistrationInfo> list;
      int                    size;
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
    } finally {
      readLock.unlock();
    }
  }

  public void remove(String address, RegistrationInfo registrationInfo) {
    Lock writeLock = republishLock.writeLock();
    writeLock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.computeIfPresent(address, (add, curr) -> removeFromSet(registrationInfo, curr));
        fireRegistrationUpdateEvent(address);
      } else {
        Set<RegistrationInfo> remoteRegistrationInfoSet = subsCache.get(address);
        if (remoteRegistrationInfoSet == null) {
          remoteRegistrationInfoSet = new HashSet<>();
        }
        remoteRegistrationInfoSet.remove(registrationInfo);
        if (remoteRegistrationInfoSet.isEmpty()) {
          subsCache.fastRemove(address);
        } else {
          subsCache.fastPut(address, remoteRegistrationInfoSet, 10, TimeUnit.SECONDS);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private Set<RegistrationInfo> removeFromSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> curr) {
    curr.remove(registrationInfo);
    return curr.isEmpty() ? null : curr;
  }

  @Override
  public void onCreated(EntryEvent<String, RegistrationInfo> event) {
    this.onEntryEvent(event);
  }

  @Override
  public void onUpdated(EntryEvent<String, RegistrationInfo> event) {
    this.onEntryEvent(event);
  }

  @Override
  public void onExpired(EntryEvent<String, RegistrationInfo> event) {
    this.onEntryEvent(event);
  }

  @Override
  public void onRemoved(EntryEvent<String, RegistrationInfo> event) {
    this.onEntryEvent(event);
  }

  private void onEntryEvent(EntryEvent<String, RegistrationInfo> event) {
    String address = event.getKey();
    vertx.<List<RegistrationInfo>> executeBlocking(prom -> prom.complete(this.get(address)), false, ar -> {
      if (ar.succeeded()) {
        nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, ar.result()));
      } else {
        log.trace("A failure occured while retrieving the updated registrations", ar.cause());
        nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, Collections.emptyList()));
      }
    });
  }

  private void fireRegistrationUpdateEvent(String address) {
    nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, this.get(address)));
  }

}
