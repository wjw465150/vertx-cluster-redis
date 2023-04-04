# Redis Cluster Manager

Redis Cluster Manager Address: https://github.com/wjw465150/vertx-cluster-redis

This is a cluster manager implementation for Vert.x that uses [Redisson](https://github.com/redisson/redisson/).

It implements interfaces of vert.x cluster totally. So you can using it to instead of vertx-hazelcast if you want. This implementation is packaged inside:

```xml
<dependency>
<groupId>com.github.wjw465150</groupId>
<artifactId>vertx-cluster-redis</artifactId>
<version>4.4.0</version>
</dependency>
```

In Vert.x a cluster manager is used for various functions including:

- Discovery and group membership of Vert.x nodes in a cluster
- Maintaining cluster wide topic subscriber lists (so we know which nodes are interested in which event busaddresses)
- Distributed Map support
- Distributed Locks
- Distributed Counters

Cluster managersdo not* handle the event bus inter-node transport, this is done directly by Vert.x with TCP connections.

## How to work

We are using [Redisson](https://github.com/redisson/redisson/) framework,you can find all the vert.x node information in hash of `__vertx:cluster:nodes`, `__vertx:asyncmaps:$name` record all the `AsyncMap` you created with `io.vertx.core.shareddata.AsyncMap` interface.  

## Using this cluster manager

If you are using Vert.x from the command line, the jar corresponding to this cluster manager (it will be named `vertx-cluster-redis-4.4.0.jar`  should be in the `lib` directory of the Vert.x installation.

If you want clustering with this cluster manager in your Vert.x Maven or Gradle project then just add a dependency to the artifact: `com.github.wjw465150:vertx-cluster-redis:${version}` in your project.

If the jar is on your classpath as above then Vert.x will automatically detect this and use it as the cluster manager. Please make sure you don’t have any other cluster managers on your classpath or Vert.x might choose the wrong one.

You can also specify the cluster manager programmatically if you are embedding Vert.x by specifying it on the options when you are creating your Vert.x instance, for example:

```java
ClusterManager mgr = new RedisClusterManager();
VertxOptions options = new VertxOptions().setClusterManager(mgr);
Vertx.clusteredVertx(options).onComplete(res -> {
  if (res.succeeded()) {
    Vertx vertx = res.result();
  } else {
    // failed!
  }
});
```

## Configuring this cluster manager

Usually the cluster manager is configured by a file [`default-redis.json`](https://github.com/wjw465150/vertx-cluster-redis/blob/master/src/main/resources/default-redis.json) which is packaged inside the jar.

If you want to override this configuration you can provide a file called `redis.json` on your classpath and this will be used instead. If you want to embed the `redis.json` file in a fat jar, it must be located at the root of the fat jar. If it’s an external file, the*directory** containing the file must be added to the classpath. For example, if you are using the *launcher* class from Vert.x, the classpath enhancement can be done as follows:

```
# If the redis.json is in the current directory:
java -jar ... -cp . -cluster
vertx run MyVerticle -cp . -cluster

# If the redis.json is in the conf directory
java -jar ... -cp conf -cluster
```

Another way to override the configuration is by providing the system property `vertx.redis.conf` with a location:

```
# Use a cluster configuration located in an external file
java -Dvertx.redis.config=./config/my-redis-conf.json -jar ... -cluster

# Or use a custom configuration from the classpath
java -Dvertx.redis.config=classpath:my/package/config/my-cluster-config.json -jar ... -cluster
```

The `vertx.redis.config` system property, when present, overrides any `redis.json` from the classpath, but if loading from this system property fails, then loading falls back to either `redis.json` or the Redis default configuration.

The configuration file is described in detail in `default-redis.json’s comment.

You can also specify configuration programmatically if embedding:

```java
JsonObject singleServerConfig = new JsonObject();
singleServerConfig.put("address", "redis://127.0.0.1:6379");
singleServerConfig.put("password", null);
    
JsonObject redisConfig = new JsonObject();
redisConfig.put("singleServerConfig", singleServerConfig);

ClusterManager mgr = new RedisClusterManager(zkConfig);
VertxOptions options = new VertxOptions().setClusterManager(mgr);

Vertx.clusteredVertx(options).onComplete(res -> {
  if (res.succeeded()) {
    Vertx vertx = res.result();
  } else {
    // failed!
  }
});
```

------

<<<<<< [完] >>>>>>
