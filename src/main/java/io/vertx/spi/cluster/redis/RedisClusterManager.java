package io.vertx.spi.cluster.redis;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.spi.cluster.redis.impl.ConfigUtil;
import io.vertx.core.json.JsonObject;

public class RedisClusterManager implements ClusterManager {
  private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

  private VertxInternal vertx;
  private NodeSelector nodeSelector;  
  
  private NodeListener nodeListener;
  private RSet<String> clusterNodes;
  private volatile boolean active;
  private volatile boolean joined;
  
  
  private String nodeId;
  private NodeInfo nodeInfo;
  private RedissonClient redisson;  //对应: private CuratorFramework curator;
  private boolean customCuratorCluster;
  private final Map<String, NodeInfo> localNodeInfo = new ConcurrentHashMap<>();
  private final Map<String, RedisLock> locks = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();  //目的是缓存已经创建的AsyncMap,为了提高速度
  private final Map<String, Map<?, ?>> mapCache = new ConcurrentHashMap<>();  //目的是缓存已经创建的Map,为了提高速度
  
  private JsonObject conf = new JsonObject();
  
  private static final String ZK_PATH_LOCKS = "__vertx:locks:/";
  private static final String CLUSTER_NODES = "__vertx:cluster:nodes:";  //private static final String ZK_PATH_CLUSTER_NODE = "/cluster/nodes/";
  
  private static final String CLUSTER_MAP_NAME = "__vertx:maps:"; 
  private static final String SUBS_MAP_NAME = "__vertx:subs:";

  private ExecutorService lockReleaseExec;

////////////////自己的  
  private final Factory factory;

  
  private Map<String, String> haInfo;
  private AsyncMap<String, NodeInfo> subs;
  //现在看来没有? private RMap<String, NodeInfo> rNodeInfo;

  private Function<String, String> resolveNodeId = path -> {
    String[] pathArr = path.split(":");
    return pathArr[pathArr.length - 1];
  };

  public RedisClusterManager() throws IOException {
    this((String) null);
  }
  
  public RedisClusterManager(String resourceLocation) throws IOException {
    conf = ConfigUtil.loadConfig(resourceLocation);
    Config config = Config.fromJSON(conf.encode());

    this.redisson = Redisson.create(config);
    this.nodeId = UUID.randomUUID().toString();
    this.factory = Factory.createDefaultFactory();
  }
  
  public RedisClusterManager(RedissonClient redisson) {
    this(redisson, UUID.randomUUID().toString());
  }

  public RedisClusterManager(RedissonClient redisson, String nodeId) {
    Objects.requireNonNull(redisson, "redisson");
    this.redisson = redisson;
    this.nodeId = nodeId;
    this.factory = Factory.createDefaultFactory();
  }


  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
  }
  
  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    vertx.executeBlocking(future -> {
      @SuppressWarnings("unchecked")
      AsyncMap<K, V> asyncMap = (AsyncMap<K, V>) asyncMapCache.computeIfAbsent(name,
          key -> factory.createAsyncMap(vertx, redisson, name));
      future.complete(asyncMap);
    }, promise);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    Map<K, V> map = (Map<K, V>) mapCache.computeIfAbsent(name,
        key -> factory.createMap(vertx, redisson, name));
    return map;
  }
  
  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    RedisLock lock = locks.get(name);
    if (lock != null) {
      promise.complete(lock);
      return;
    }
    
    RLock rLock = redisson.getLock(name);
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
      RAtomicLong counter = redisson.getAtomicLong(name);
      promise.complete(new RedisCounter(counter));
    } catch (Exception e) {
      log.warn(MessageFormat.format("nodeId: {0}, counter name: {1}", nodeId, name), e);
      promise.fail(e);
    }
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  /**
   * @see io.vertx.core.impl.HAManager#addHaInfoIfLost
   */
  @Override
  public List<String> getNodes() {
    List<String> nodes = clusterNodes.stream().collect(Collectors.toList());
    if (nodes.isEmpty()) {
      log.warn(MessageFormat.format("(nodes.isEmpty()), nodeId: {0}", nodeId));
    } else {
 			log.debug(MessageFormat.format("nodeId: {0}, nodes.size: {1}, nodes: {2}", nodeId, nodes.size(), nodes));
 		}
    return nodes;
  }
  
  @Override
  public void nodeListener(NodeListener nodeListener) {
    this.nodeListener = nodeListener;
  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
    synchronized (this) {
      this.nodeInfo = nodeInfo;
    }
    
    try {
      RBucket<NodeInfo> rBucketNodeInfo = redisson.getBucket(CLUSTER_NODES+nodeId);
      rBucketNodeInfo.setAsync(nodeInfo).whenComplete((rv,e) -> {
        if(e != null) {
          log.error(MessageFormat.format("setNodeInfo nodeId: {0}", nodeId), e);
          promise.fail(e);
        } else {
          localNodeInfo.put(nodeId, nodeInfo);
          promise.complete();
        }
      });
    } catch (Exception e) {
      log.error("setNodeInfo failed.", e);
      promise.fail(e);
    }
    
  }

  @Override
  public synchronized NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
    RBucket<NodeInfo> rBucketNodeInfo = redisson.getBucket(CLUSTER_NODES+nodeId);
    rBucketNodeInfo.getAsync().whenComplete((rv,e) -> {
      if(e != null) {
        log.error(MessageFormat.format("getNodeInfo nodeId: {0}", nodeId), e);
        promise.fail(e);
      } else {
        promise.complete(rv);
      }
    });
  }
  
  @Override
  public void join(Promise<Void> promise) {
    if (!active) {
      active = true;
      lockReleaseExec = Executors.newCachedThreadPool(r -> new Thread(r, "vertx-redis-service-release-lock-thread"));
     
      this.nodeId = UUID.randomUUID().toString();
      promise.complete();
    } else {
      log.warn(MessageFormat.format("Already activated, nodeId: {0}", nodeId));
      promise.fail(new IllegalStateException(MessageFormat.format("Already activated, nodeId: {0}", nodeId)));
    }
  }

  @Override
  public void leave(Promise<Void> promise) {
    
    synchronized (RedisClusterManager.this) {
      if (active) {
        active = false;
        joined = false;
        lockReleaseExec.shutdown();
        try {
          redisson.shutdown();
        } catch (Exception e) {
          log.warn(MessageFormat.format("Already activated, nodeId: {0}", nodeId));
          promise.fail(new IllegalStateException(MessageFormat.format("Already activated, nodeId: {0}", nodeId)));
        } finally {
          promise.complete();
        }
      } else {
        promise.complete();
      }
    }
    
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public void addRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void removeRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public String toString() {
    return MessageFormat.format("Redis Cluster Manager {nodeID={0}}", getNodeId());
  }

  /**
   * Lock implement
   */
  private class RedisCounter implements Counter {
    private final RAtomicLong counter;

    public RedisCounter(RAtomicLong counter) {
      this.counter = counter;
    }

    @Override
    public Future<Long> get() {
      Promise<Long> promise = Promise.promise();
      Context       context = vertx.getOrCreateContext();
      counter.getAsync().whenComplete((rv, e) -> 
        context.runOnContext(vd -> {
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
      counter.incrementAndGetAsync().whenComplete((rv, e) -> 
        context.runOnContext(vd -> {
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
      counter.getAndIncrementAsync().whenComplete((rv, e) -> 
        context.runOnContext(vd -> {
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
      counter.decrementAndGetAsync().whenComplete((rv, e) -> 
        context.runOnContext(vd -> {
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
      counter.addAndGetAsync(value).whenComplete((rv, e) -> 
        context.runOnContext(vd -> {
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
      counter.getAndAddAsync(value).whenComplete((rv, e) -> 
        context.runOnContext(vd -> {
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
      Context       context = vertx.getOrCreateContext();
      counter.compareAndSetAsync(expected,value).whenComplete((rv, e) -> 
        context.runOnContext(vd -> {
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

  /**
   * Lock implement
   */
  private class RedisLock implements io.vertx.core.shareddata.Lock {
    private final RLock lock;

    public RedisLock(RLock lock) {
      this.lock = lock;
    }

    @Override
    public void release() {
      lock.unlock();
    }
  }

}
