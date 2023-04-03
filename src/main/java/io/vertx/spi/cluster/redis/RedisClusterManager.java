package io.vertx.spi.cluster.redis;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.spi.cluster.redis.impl.ConfigUtil;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMap;
import io.vertx.spi.cluster.redis.impl.RedisCounter;
import io.vertx.spi.cluster.redis.impl.RedisLock;
import io.vertx.spi.cluster.redis.impl.RedisSyncMap;
import io.vertx.spi.cluster.redis.impl.SubsMapHelper;

public class RedisClusterManager implements ClusterManager, EntryCreatedListener<String, NodeInfo>, EntryUpdatedListener<String, NodeInfo>, EntryExpiredListener<String, NodeInfo>, EntryRemovedListener<String, NodeInfo> {
  private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

  private VertxInternal vertx;
  private NodeSelector  nodeSelector;

  private NodeListener                nodeListener;
  private RMapCache<String, NodeInfo> clusterNodes;
  private volatile boolean            active;

  private String                      nodeId;
  private NodeInfo                    nodeInfo;
  private RedissonClient              redisson;
  private boolean                     customRedisCluster;
  private SubsMapHelper               subsMapHelper;

  //目的是缓存已经创建的RedisLock,RedisCounter,AsyncMap,syncMapCache 为了提高速度
  private final Map<String, RedisLock>          locksCache    = new ConcurrentHashMap<>();
  private final Map<String, RedisCounter>       countersCache = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>>     asyncMapCache = new ConcurrentHashMap<>();
  private final Map<String, RedisSyncMap<?, ?>> syncMapCache  = new ConcurrentHashMap<>();

  private JsonObject conf = new JsonObject();

  private static final String VERTX_LOCKS         = "__vertx:locks:";
  private static final String VERTX_COUNTERS      = "__vertx:counters:";
  private static final String VERTX_CLUSTER_NODES = "__vertx:cluster:nodes";

  private static final String VERTX_ASYNCMAPS = "__vertx:asyncmaps:";
  private static final String VERTX_SYNCMAPS  = "__vertx:syncmaps:";

  private static final int ENTRY_TTL = 10;
  private final ScheduledExecutorService nodesTtlScheduler = Executors.newScheduledThreadPool(1);

  /**
   * Constructor - gets config from classpath
   */
  public RedisClusterManager() throws IOException {
    conf = ConfigUtil.loadConfig(null);
  }

  public RedisClusterManager(RedissonClient redisson) {
    this(redisson, UUID.randomUUID().toString());
  }

  public RedisClusterManager(String resourceLocation) throws IOException {
    conf = ConfigUtil.loadConfig(resourceLocation);
  }

  public RedisClusterManager(RedissonClient redisson, String nodeId) {
    Objects.requireNonNull(redisson, "redisson");
    Objects.requireNonNull(nodeId, "The nodeId cannot be null.");
    this.redisson = redisson;
    this.nodeId = nodeId;
    this.customRedisCluster = true;
  }

  public RedisClusterManager(JsonObject config) {
    this.conf = config;
  }

  public void setConfig(JsonObject conf) {
    this.conf = conf;
  }

  public JsonObject getConfig() {
    return conf;
  }

  public RedissonClient getRedissonClient() {
    return this.redisson;
  }

  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
  }

  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    vertx.executeBlocking(prom -> {
      @SuppressWarnings("unchecked")
      AsyncMap<K, V> zkAsyncMap = (AsyncMap<K, V>) asyncMapCache.computeIfAbsent(name, k -> new RedisAsyncMap<>(vertx, redisson.getMapCache(VERTX_ASYNCMAPS + name)));
      prom.complete(zkAsyncMap);
    }, promise);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    @SuppressWarnings("unchecked")
    RedisSyncMap<K, V> map = (RedisSyncMap<K, V>) syncMapCache.computeIfAbsent(name, k -> new RedisSyncMap<>(redisson.getMapCache(VERTX_SYNCMAPS + name)));
    return map;
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    RedisLock lock = locksCache.get(VERTX_LOCKS + name);
    if (lock != null) {
      promise.complete(lock);
      return;
    }

    RLock     rLock   = redisson.getLock(name);
    RedisLock newLock = new RedisLock(rLock);
    rLock.tryLockAsync(timeout, TimeUnit.MILLISECONDS).whenComplete((rv, e) -> {
      if (e != null) {
        log.warn(MessageFormat.format("nodeId: {0}, lock name: {1}, timeout: {2}", nodeId, name, timeout), e);
        promise.fail(e);
      } else {
        locksCache.putIfAbsent(name, newLock);
        promise.complete(newLock);
      }
    });
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    vertx.executeBlocking(prom -> {
      RedisCounter counter = countersCache.computeIfAbsent(name, k -> new RedisCounter(vertx, redisson.getAtomicLong(VERTX_COUNTERS + name)));
      promise.complete(counter);
    }, promise);
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public List<String> getNodes() {
    List<String> nodes = clusterNodes.keySet().stream().collect(Collectors.toList());
    if (nodes.isEmpty()) {
      log.warn(MessageFormat.format("(nodes.isEmpty()), nodeId: {0}", nodeId));
    } else {
      log.debug(MessageFormat.format("nodeId: {0}, nodes.size: {1}, nodes: {2}", nodeId, nodes.size(), nodes));
    }
    return nodes;
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
    synchronized (this) {
      this.nodeInfo = nodeInfo;
    }
    vertx.executeBlocking(prom -> {
      clusterNodes.put(nodeId, nodeInfo, ENTRY_TTL, TimeUnit.SECONDS);
      prom.complete();
    }, false, promise);
  }

  @Override
  public synchronized NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
    vertx.executeBlocking(prom -> {
      NodeInfo remoteNodeInfo = clusterNodes.get(nodeId);
      if (remoteNodeInfo != null) {
        prom.complete(remoteNodeInfo);
      } else {
        promise.fail("Not a member of the cluster");
      }
    }, false, promise);
  }

  private void addLocalNodeId() throws VertxException {
    clusterNodes = redisson.getMapCache(VERTX_CLUSTER_NODES, JsonJacksonCodec.INSTANCE);
    clusterNodes.addListener(this);
    try {
      if (nodeInfo != null) {
        clusterNodes.put(nodeId, nodeInfo, ENTRY_TTL, TimeUnit.SECONDS);
      }
      subsMapHelper = new SubsMapHelper(vertx, redisson, nodeSelector, nodeId);

      nodesTtlScheduler.scheduleAtFixedRate(() -> {
        if (nodeId != null) {
          clusterNodes.updateEntryExpiration(nodeId, ENTRY_TTL, TimeUnit.SECONDS, 0, TimeUnit.SECONDS);
          clusterNodes.expire(Duration.ofSeconds(ENTRY_TTL));
        }

        if (subsMapHelper != null) {
          subsMapHelper.updateSubsEntryExpiration(ENTRY_TTL, TimeUnit.SECONDS);
        }
      }, 500, 500, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx.executeBlocking(prom -> {
      if (!active) {
        active = true;

        //The redis instance has been passed using the constructor.
        if (customRedisCluster) {
          try {
            addLocalNodeId();
            prom.complete();
          } catch (VertxException e) {
            prom.fail(e);
          }
          return;
        }

        try {
          if (this.redisson == null) {
            Config config = Config.fromJSON(conf.encode());
            this.redisson = Redisson.create(config);
          }

          nodeId = UUID.randomUUID().toString();
          addLocalNodeId();
          prom.complete();
        } catch (Exception e) {
          prom.fail(e);
        }
      } else {
        prom.complete();
      }
    }, promise);
  }

  @Override
  public void leave(Promise<Void> promise) {
    vertx.executeBlocking(prom -> {
      // We need to synchronized on the cluster manager instance to avoid other call to happen while leaving the
      // cluster, typically, memberRemoved and memberAdded
      synchronized (RedisClusterManager.this) {
        if (active) {
          active = false;
          nodesTtlScheduler.shutdown();

          try {
            clusterNodes.remove(nodeId);
            subsMapHelper.close();
            subsMapHelper = null;
            if (!customRedisCluster) {
              redisson.shutdown();
              redisson = null;
            }
          } catch (Exception ex) {
            prom.fail(ex);
          } finally {
            prom.complete();
          }
        } else {
          prom.complete();
        }
      }
    }, promise);
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public void addRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    subsMapHelper.put(address, registrationInfo, promise);
  }

  @Override
  public void removeRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    subsMapHelper.remove(address, registrationInfo, promise);
  }

  @Override
  public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
    vertx.executeBlocking(prom -> {
      prom.complete(subsMapHelper.get(address));
    }, false, promise);
  }

  @Override
  public String toString() {
    return MessageFormat.format("Redis Cluster Manager {nodeID={0}}", getNodeId());
  }

  @Override
  public void onCreated(EntryEvent<String, NodeInfo> event) {
    if (!active)
      return;

    try {
      if (nodeListener != null) {
        nodeListener.nodeAdded(event.getKey());
      }
    } catch (Throwable t) {
      log.error("Failed to handle memberAdded", t);
    }
  }

  @Override
  public void onRemoved(EntryEvent<String, NodeInfo> event) {
    if (!active)
      return;

    try {
      if (nodeListener != null) {
        nodeListener.nodeLeft(event.getKey());
      }
    } catch (Throwable t) {
      log.warn("Failed to handle memberRemoved", t);
    }
  }

  @Override
  public void onExpired(EntryEvent<String, NodeInfo> event) {
    this.onRemoved(event);
  }

  @Override
  public void onUpdated(EntryEvent<String, NodeInfo> event) {
    if (!active)
      return;
  }

}
