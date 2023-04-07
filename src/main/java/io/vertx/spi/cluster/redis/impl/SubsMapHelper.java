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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.redisson.api.RMultimapCache;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;

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
  private final ConcurrentMap<String, Set<RegistrationInfo>> localSubs = new ConcurrentHashMap<>();
  private final ReadWriteLock                                republishLock;

  private static final String VERTX_SUBS_NAME  = "__vertx:subs";
  private static final String VERTX_SUBS_LOCKS = "__vertx:subs_locks";

  public SubsMapHelper(VertxInternal vertx, RedissonClient redisson, NodeSelector nodeSelector, String nodeId) {
    this.vertx = vertx;
    this.redisson = redisson;
    this.subsCache = redisson.getSetMultimapCache(VERTX_SUBS_NAME, JsonJacksonCodec.INSTANCE);
    this.republishLock = redisson.getReadWriteLock(VERTX_SUBS_LOCKS);

    this.nodeSelector = nodeSelector;
    this.nodeId = nodeId;

  }

  public void updateSubsEntryExpiration(long ttl, TimeUnit ttlUnit) {
    try {
      Iterator<String> subsKeySetIterator = subsCache.keySet().iterator();
      while (subsKeySetIterator.hasNext()) {
        subsCache.expireKey(subsKeySetIterator.next(), ttl, ttlUnit);
      }
    } catch (Exception e) {
      //e.printStackTrace();
    }
  }

  public void close() {
    //
  }

  public void put(String address, RegistrationInfo registrationInfo) {
    Lock writeLock = republishLock.writeLock();
    writeLock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));
      } else {
        try {
          subsCache.put(address, registrationInfo);
        } catch (Exception e) {
          log.error(String.format("create subs address %s failed.", address), e);
        }
      }
      nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, this.get(address)));
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
      } else {
        //-> @wjw_add 删除指令来的早了,节点还不存在,这时候重试几次!
        int retryCount=0;
        boolean isOK = subsCache.remove(address,registrationInfo);
        while(!isOK && retryCount<3) {
          log.warn(MessageFormat.format("要删除的Zookeeper节点不存在:{0}, 重试第:{1}次!", address, retryCount));
          java.util.concurrent.TimeUnit.SECONDS.sleep(1);
          retryCount++;
          isOK = subsCache.remove(address,registrationInfo);
        }
        if(!isOK) {
          log.warn(MessageFormat.format("重试几次后,要删除的Zookeeper节点还不存在:{0}", address));
        }
        //<- @wjw_add
      }
      nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, this.get(address)));
    } catch (Exception e) {
      log.error(String.format("remove subs address %s failed.", address), e);
    } finally {
      writeLock.unlock();
    }
  }

  private Set<RegistrationInfo> removeFromSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> curr) {
    curr.remove(registrationInfo);
    return curr.isEmpty() ? null : curr;
  }

}
