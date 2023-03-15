/*
 * Copyright (c) 2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.spi.cluster.redis;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class RetriableReceiver2Test extends RetriableReceiverTest {
  private static final Logger log = LoggerFactory.getLogger(RetriableReceiver2Test.class);

  @Override
  protected int clusterPort() {
    return 38080;
  }

  @Test
  @Override
  public void test1EventBusP2P() throws Exception {
    log.debug("BEGIN...");
    super.test1EventBusP2P();
  }
}
