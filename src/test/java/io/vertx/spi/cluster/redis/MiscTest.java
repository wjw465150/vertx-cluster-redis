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

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class MiscTest {
  private static final Logger log = LoggerFactory.getLogger(MiscTest.class);

  @Test
  public void test1ConcurrentMap() throws Exception {
    final ConcurrentMap<String, String> cmap = new ConcurrentHashMap<>();

    cmap.computeIfAbsent("ABC", key -> {
      log.debug("key={}", key);
      return "123";
    });
    log.debug("cmap.size={}, cmap={}", cmap.size(), cmap);
    Assertions.assertEquals(cmap.get("ABC"), "123");

    cmap.computeIfAbsent("DEF", key -> {
      log.debug("key={}", key);
      return null;
    });
    log.debug("cmap.size={}, cmap={}", cmap.size(), cmap);
    Assertions.assertNull(cmap.get("DEF"));

    Assertions.assertEquals(cmap.size(), 1);
  }

  @Test
  public void test2Scoe() throws Exception {
    // final DateTimeFormatter iso8601WithMillisFormatter =
    // DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx");
    final DateTimeFormatter iso8601WithMillisFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    //		long lastAccessed = new Date().getTime();
    long lastAccessed = 1523248556694L;

    OffsetDateTime dateTime = OffsetDateTime.ofInstant(new Date(lastAccessed).toInstant(), ZoneId.systemDefault());
    String iso8601Str = dateTime.format(iso8601WithMillisFormatter);
    log.debug("lastAccessed ={}, iso8601Str={}", lastAccessed, iso8601Str);

    ZonedDateTime zonedDateTime = ZonedDateTime.parse(iso8601Str, iso8601WithMillisFormatter);
    long lastAccessed2 = Date.from(zonedDateTime.toInstant()).getTime();

    log.debug("lastAccessed2={}, iso8601Str={}", lastAccessed2, iso8601Str);

    Assertions.assertEquals(lastAccessed, lastAccessed2);
  }
}
