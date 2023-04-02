package io.vertx.spi.cluster.redis;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
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
import io.vertx.spi.cluster.redis.impl.SubsMapHelper;

public class RedisClusterManager implements ClusterManager, EntryCreatedListener<String, NodeInfo>, EntryUpdatedListener<String, NodeInfo>, EntryExpiredListener<String, NodeInfo>, EntryRemovedListener<String, NodeInfo> {
  private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

  private VertxInternal vertx;
  private NodeSelector  nodeSelector;

  private NodeListener                nodeListener;
  private RMapCache<String, NodeInfo> clusterNodes;
  private volatile boolean            active;
  private volatile boolean            joined;

  private String                            nodeId;
  private NodeInfo                          nodeInfo;
  private RedissonClient                    redisson;
  private boolean                           customRedisCluster;
  private SubsMapHelper                     subsMapHelper;
  private final Map<String, NodeInfo>       localNodeInfo = new ConcurrentHashMap<>();
  private final Map<String, RedisLock>      locks         = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>(); //目的是缓存已经创建的AsyncMap,为了提高速度
  private final Map<String, Map<?, ?>>      mapCache      = new ConcurrentHashMap<>(); //目的是缓存已经创建的Map,为了提高速度

  private JsonObject conf = new JsonObject();

  private static final String CLUSTER_LOCKS    = "__vertx:locks:";
  private static final String CLUSTER_COUNTERS = "__vertx:counters:";
  private static final String CLUSTER_NODES    = "__vertx:cluster:nodes:";

  private static final String CLUSTER_ASYNC_MAP_NAME = "__vertx:asyncmaps:";
  private static final String CLUSTER_SYNC_MAP_NAME  = "__vertx:syncmaps:";

  private ExecutorService lockReleaseExec;

  /**
   * Constructor - gets config from classpath
   */
  public RedisClusterManager() throws IOException {
    this((String) null);
  }

  public RedisClusterManager(String resourceLocation) throws IOException {
    conf = ConfigUtil.loadConfig(resourceLocation);

    this.nodeId = UUID.randomUUID().toString();
  }

  public RedisClusterManager(RedissonClient redisson) {
    this(redisson, UUID.randomUUID().toString());
  }

  public RedisClusterManager(RedissonClient redisson, String nodeId) {
    Objects.requireNonNull(redisson, "redisson");
    this.redisson = redisson;
    this.nodeId = nodeId;
    this.customRedisCluster = true;
  }

  public void setConfig(JsonObject conf) {
    this.conf = conf;
  }

  public JsonObject getConfig() {
    return conf;
  }

  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
  }

  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    promise.complete(new RedisAsyncMap<>(vertx, redisson.getMapCache(CLUSTER_ASYNC_MAP_NAME + name)));
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return redisson.getMap(CLUSTER_SYNC_MAP_NAME + name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    RedisLock lock = locks.get(CLUSTER_LOCKS + name);
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
        locks.putIfAbsent(name, newLock);
        promise.complete(newLock);
      }
    });
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    try {
      RAtomicLong counter = redisson.getAtomicLong(CLUSTER_COUNTERS + name);
      promise.complete(new RedisCounter(vertx, counter));
    } catch (Exception e) {
      log.warn(MessageFormat.format("nodeId: {0}, counter name: {1}", nodeId, name), e);
      promise.fail(e);
    }
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
      localNodeInfo.put(nodeId, nodeInfo);
      clusterNodes.put(nodeId, nodeInfo);
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
    clusterNodes = redisson.getMapCache(CLUSTER_NODES);
    clusterNodes.addListener(this);
    try {
      //Join to the cluster
      createThisNode();
      joined = true;
      subsMapHelper = new SubsMapHelper(vertx, redisson, nodeSelector, nodeId);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  private void createThisNode() throws Exception {
    clusterNodes.put(nodeId, nodeInfo);
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx.executeBlocking(prom -> {
      if (!active) {
        active = true;
        lockReleaseExec = Executors.newCachedThreadPool(r -> new Thread(r, "vertx-redis-service-release-lock-thread"));

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
          try {
            active = false;
            joined = false;
            lockReleaseExec.shutdown();

            clusterNodes.remove(nodeId);
            subsMapHelper.close();
            if (!customRedisCluster) {
              redisson.shutdown();
              redisson = null;
            }
          } catch (Exception ex) {
            prom.fail(ex);
          }
        }
      }
      prom.complete();
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
