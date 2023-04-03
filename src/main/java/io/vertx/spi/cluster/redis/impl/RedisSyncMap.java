/*
 * author: @wjw
 * date:   2023年4月3日 上午10:18:00
 * note: 
 */
package io.vertx.spi.cluster.redis.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.redisson.api.RMap;

public class RedisSyncMap<K, V> implements Map<K, V> {

  private final RMap<K, V>    map;

  public RedisSyncMap(RMap<K, V> map) {
    this.map = map;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return map.get(key);
  }

  @Override
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    map.putAll(m);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<K> keySet() {
    return map.readAllKeySet();
  }

  @Override
  public Collection<V> values() {
    return map.readAllValues();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return map.readAllEntrySet();
  }

}
