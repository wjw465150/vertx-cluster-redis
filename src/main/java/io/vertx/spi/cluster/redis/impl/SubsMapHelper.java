package io.vertx.spi.cluster.redis.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.redisson.api.RMapCache;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;

public class SubsMapHelper implements EntryCreatedListener<String, RegistrationInfo>, EntryRemovedListener<String, RegistrationInfo>, EntryUpdatedListener<String, RegistrationInfo> {
  private static final Logger log = LoggerFactory.getLogger(SubsMapHelper.class);

  private final RedissonClient redisson;

  private final Throttling                throttling;
  /*
   * 用RMapCache是因为它有各种Entry Listener,而RMultimapCache没有
   * 但是又要实现RMultimapCache的多value,怎么办呢? 那就再用一个RSet来实现!
   * RMapCache<String, String>里key存放事件地址,value存放订阅者Set的Redis键
   * RSet<RegistrationInfo> 里存放的是特定事件地址的订阅者集合.
   * RMapCache[event address -> set address] -> RSet<RegistrationInfo> 
   */
  private final RMapCache<String, String> map;
  private final NodeSelector              nodeSelector;
  private final int                       listenerId;

  private final ConcurrentMap<String, Set<RegistrationInfo>> ownSubs       = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<RegistrationInfo>> localSubs     = new ConcurrentHashMap<>();
  private final ReadWriteLock                                republishLock = new ReentrantReadWriteLock();

  private static final String VERTX_SUBS_MAP_NAME = "__vertx:subs";
  private static final String VERTX_SUBS_SET_NAME = VERTX_SUBS_MAP_NAME + ":sets:";

  public SubsMapHelper(RedissonClient redisson, NodeSelector nodeSelector) {
    this.redisson = redisson;
    this.nodeSelector = nodeSelector;

    throttling = new Throttling(this::getAndUpdate);
    this.map = redisson.getMapCache(VERTX_SUBS_MAP_NAME);
    listenerId = map.addListener(this);
  }

  public List<RegistrationInfo> get(String address) {
    Lock readLock = republishLock.readLock();
    readLock.lock();
    try {
      List<RegistrationInfo> list;
      int                    size;

      String                 subSetAddress = map.computeIfAbsent(address, k -> VERTX_SUBS_SET_NAME + address);
      RSet<RegistrationInfo> remote     = redisson.getSet(subSetAddress);

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

  public void put(String address, RegistrationInfo registrationInfo) {
    Lock readLock = republishLock.readLock();
    readLock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));
        fireRegistrationUpdateEvent(address);
      } else {
        ownSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));

        String                 subSetAddress = map.computeIfAbsent(address, k -> VERTX_SUBS_SET_NAME + address);
        RSet<RegistrationInfo> remote     = redisson.getSet(subSetAddress);
        remote.add(registrationInfo);
      }
    } finally {
      readLock.unlock();
    }
  }

  private Set<RegistrationInfo> addToSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> curr) {
    Set<RegistrationInfo> res = curr != null ? curr : Collections.synchronizedSet(new LinkedHashSet<>());
    res.add(registrationInfo);
    return res;
  }

  public void remove(String address, RegistrationInfo registrationInfo) {
    Lock readLock = republishLock.readLock();
    readLock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.computeIfPresent(address, (add, curr) -> removeFromSet(registrationInfo, curr));
        fireRegistrationUpdateEvent(address);
      } else {
        ownSubs.computeIfPresent(address, (add, curr) -> removeFromSet(registrationInfo, curr));

        String                 subSetAddress = map.computeIfAbsent(address, k -> VERTX_SUBS_SET_NAME + address);
        RSet<RegistrationInfo> remote     = redisson.getSet(subSetAddress);
        remote.remove(registrationInfo);
      }
    } finally {
      readLock.unlock();
    }
  }

  private Set<RegistrationInfo> removeFromSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> curr) {
    curr.remove(registrationInfo);
    return curr.isEmpty() ? null : curr;
  }

  public void removeAllForNodes(Set<String> nodeIds) {
    map.values().forEach(subSetAddress -> {
      RSet<RegistrationInfo>     remote   = redisson.getSet(subSetAddress);
      Iterator<RegistrationInfo> iterator = remote.iterator();
      while (iterator.hasNext()) {
        RegistrationInfo registrationInfo = iterator.next();
        if (nodeIds.contains(registrationInfo.nodeId())) {
          remote.remove(registrationInfo);
        }
      }
    });
  }

  public void republishOwnSubs() {
    Lock writeLock = republishLock.writeLock();
    writeLock.lock();
    try {
      for (Map.Entry<String, Set<RegistrationInfo>> entry : ownSubs.entrySet()) {
        String address    = entry.getKey();
        String subSetAddress = VERTX_SUBS_SET_NAME + address;
        map.put(address, subSetAddress);
        RSet<RegistrationInfo> remote = redisson.getSet(subSetAddress);
        for (RegistrationInfo registrationInfo : entry.getValue()) {
          remote.add(registrationInfo);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void fireRegistrationUpdateEvent(String address) {
    throttling.onEvent(address);
  }

  private void getAndUpdate(String address) {
    if (nodeSelector.wantsUpdatesFor(address)) {
      List<RegistrationInfo> registrationInfos;
      try {
        registrationInfos = get(address);
      } catch (Exception e) {
        log.trace("A failure occurred while retrieving the updated registrations", e);
        registrationInfos = Collections.emptyList();
      }
      nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, registrationInfos));
    }
  }

  @Override
  public void onCreated(EntryEvent<String, RegistrationInfo> event) {
    fireRegistrationUpdateEvent(event.getKey());
  }

  @Override
  public void onRemoved(EntryEvent<String, RegistrationInfo> event) {
    fireRegistrationUpdateEvent(event.getKey());
  }

  @Override
  public void onUpdated(EntryEvent<String, RegistrationInfo> event) {
    fireRegistrationUpdateEvent(event.getKey());
  }

  public void close() {
    map.removeListener(listenerId);
    throttling.close();
  }

}
