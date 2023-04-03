/*
 * author: @wjw
 * date:   2023年4月3日 下午4:01:18
 * note: 
 */
package io.vertx.core.eventbus;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Test;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.MockRedisCluster;
import io.vertx.test.core.TestUtils;

public class RedisClusteredEventbusTest extends ClusteredEventBusTest {

  private final MockRedisCluster redisClustered = new MockRedisCluster();

  @Override
  protected ClusterManager getClusterManager() {
    return redisClustered.getClusterManager();
  }

  @Override
  protected VertxOptions getOptions() {
    VertxOptions options = super.getOptions();
    //防止调试的时候出现`BlockedThreadChecker`日志信息
    long blockedThreadCheckInterval = 60 * 60 * 1000L;
    options.setBlockedThreadCheckInterval(blockedThreadCheckInterval);

    return options;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    redisClustered.stop();
  }

  public void await(long delay, TimeUnit timeUnit) {
    //fail fast if test blocking
    super.await(10, TimeUnit.SECONDS);
  }

  @Override
  protected <T, R> void testSend(T val, R received, Consumer<T> consumer, DeliveryOptions options) {
    if (vertices == null) {
      startNodes(2);
    }

    MessageConsumer<T> reg = vertices[1].eventBus().<T> consumer(ADDRESS1).handler((Message<T> msg) -> {
      if (consumer == null) {
        assertTrue(msg.isSend());
        assertEquals(received, msg.body());
        if (options != null) {
          assertNotNull(msg.headers());
          int numHeaders = options.getHeaders() != null ? options.getHeaders().size() : 0;
          assertEquals(numHeaders, msg.headers().size());
          if (numHeaders != 0) {
            for (Map.Entry<String, String> entry : options.getHeaders().entries()) {
              assertEquals(msg.headers().get(entry.getKey()), entry.getValue());
            }
          }
        }
      } else {
        consumer.accept(msg.body());
      }
      testComplete();
    });
    reg.completionHandler(ar -> {
      assertTrue(ar.succeeded());
      vertices[1].setTimer(200L, along -> {
        if (options == null) {
          vertices[0].eventBus().send(ADDRESS1, val);
        } else {
          vertices[0].eventBus().send(ADDRESS1, val, options);
        }
      });
    });
    await();
  }

  @Override
  protected <T, R> void testReply(T val, R received, Consumer<R> consumer, DeliveryOptions options) {
    if (vertices == null) {
      startNodes(2);
    }
    String             str = TestUtils.randomUnicodeString(1000);
    MessageConsumer<?> reg = vertices[1].eventBus().consumer(ADDRESS1).handler(msg -> {
                             assertEquals(str, msg.body());
                             if (options == null) {
                               msg.reply(val);
                             } else {
                               msg.reply(val, options);
                             }
                           });
    reg.completionHandler(ar -> {
      assertTrue(ar.succeeded());
      vertices[1].setTimer(200L, along -> {
        vertices[0].eventBus().request(ADDRESS1, str, onSuccess((Message<R> reply) -> {
          if (consumer == null) {
            assertTrue(reply.isSend());
            assertEquals(received, reply.body());
            if (options != null && options.getHeaders() != null) {
              assertNotNull(reply.headers());
              assertEquals(options.getHeaders().size(), reply.headers().size());
              for (Map.Entry<String, String> entry : options.getHeaders().entries()) {
                assertEquals(reply.headers().get(entry.getKey()), entry.getValue());
              }
            }
          } else {
            consumer.accept(reply.body());
          }
          testComplete();
        }));
      });
    });
    await();
  }

  @Override
  protected <T> void testPublish(T val, Consumer<T> consumer) {
    int numNodes = 3;
    startNodes(numNodes);
    AtomicInteger count = new AtomicInteger();
    class MyHandler implements Handler<Message<T>> {
      @Override
      public void handle(Message<T> msg) {
        if (consumer == null) {
          assertFalse(msg.isSend());
          assertEquals(val, msg.body());
        } else {
          consumer.accept(msg.body());
        }
        if (count.incrementAndGet() == numNodes - 1) {
          testComplete();
        }
      }
    }
    AtomicInteger registerCount = new AtomicInteger(0);
    class MyRegisterHandler implements Handler<AsyncResult<Void>> {
      @Override
      public void handle(AsyncResult<Void> ar) {
        assertTrue(ar.succeeded());
        if (registerCount.incrementAndGet() == 2) {
          vertices[0].setTimer(300L, h -> {
            vertices[0].eventBus().publish(ADDRESS1, val);
          });
        }
      }
    }
    MessageConsumer reg = vertices[2].eventBus().<T> consumer(ADDRESS1).handler(new MyHandler());
    reg.completionHandler(new MyRegisterHandler());
    reg = vertices[1].eventBus().<T> consumer(ADDRESS1).handler(new MyHandler());
    reg.completionHandler(new MyRegisterHandler());
    await();
  }

  @Test
  public void testLocalHandlerClusteredPublish() throws Exception {
    this.startNodes(2);
    this.waitFor(2);
    this.vertices[1].eventBus().consumer("some-address1", (msg) -> {
      System.out.println("some-address1:"+msg.body());
      this.complete();
    }).completionHandler((v1) -> {
      this.vertices[0].eventBus().localConsumer("some-address1", (msg) -> {
        System.out.println("some-address1:"+msg.body());
        this.complete();
      }).completionHandler((v2) -> {
        this.vertices[0].eventBus().publish("some-address1", "foo");
      });
    });
    this.await();
  }
  
}
