/*
 * Copyright 2018 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.spi.cluster.redis;

import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.web.sstore.ClusteredSessionHandlerTest;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class RedisClusteredSessionHandlerTest extends ClusteredSessionHandlerTest {
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(RedisClusteredSessionHandlerTest.class);

  static private RedissonClient redisson;

  @BeforeAll
  static public void beforeClass() {
    Config config = new Config();
    config.useSingleServer() //
        .setAddress("redis://127.0.0.1:6379") //
        .setDatabase(1) //
        .setPassword("mypwd") //
        .setConnectionMinimumIdleSize(5);
    redisson = Redisson.create(config);
  }

  @AfterAll
  static public void afterClass() {
    redisson.shutdown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    CountDownLatch ready = new CountDownLatch(1);
    store.clear(ar -> {
      ready.countDown();
    });
    ready.await();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new RedisClusterManager(redisson);
  }

}
