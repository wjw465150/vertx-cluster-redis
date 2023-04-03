/*
 * author: @wjw
 * date:   2023年4月3日 上午10:18:19
 * note: 
 */
package io.vertx.spi.cluster.redis.impl;

import org.redisson.api.RLock;

public class RedisLock  implements io.vertx.core.shareddata.Lock {
  private final RLock lock;

  public RedisLock(RLock lock) {
    this.lock = lock;
  }

  @Override
  public void release() {
    lock.unlock();
  }
}
