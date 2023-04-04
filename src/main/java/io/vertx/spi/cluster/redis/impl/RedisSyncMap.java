/*
 * author: @wjw
 * date:   2023年4月3日 上午10:18:00
 * note: 
 */
package io.vertx.spi.cluster.redis.impl;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.codec.TypedJsonJacksonCodec;

public class RedisSyncMap<K, V> implements Map<K, V> {

  private final RMap<byte[], byte[]> map;

  public RedisSyncMap(RedissonClient redisson, String mapName) {
    this.map = redisson.getMap(mapName, new TypedJsonJacksonCodec(byte[].class, byte[].class));
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
    return map.containsKey(ConversionUtils.asByte(key));
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(ConversionUtils.asByte(value));
  }

  @Override
  public V get(Object key) {
    byte[] bb = map.get(ConversionUtils.asByte(key));
    return ConversionUtils.asObject(bb);
  }

  @Override
  public V put(K key, V value) {
    byte[] old = map.put(ConversionUtils.asByte(key), ConversionUtils.asByte(value));
    if (old == null) {
      return null;
    }
    return ConversionUtils.asObject(old);
  }

  @Override
  public V remove(Object key) {
    byte[] old = map.remove(ConversionUtils.asByte(key));
    return ConversionUtils.asObject(old);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.entrySet().stream().forEach(entry -> {
      map.put(ConversionUtils.asByte(entry.getKey()), ConversionUtils.asByte(entry.getValue()));
    });
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<K> keySet() {
    Set<byte[]> bbSet = map.readAllKeySet();
    return bbSet.stream().map(bb -> ConversionUtils.<K> asObject(bb)).collect(Collectors.toSet());
  }

  @Override
  public Collection<V> values() {
    Collection<byte[]> bValues = map.readAllValues();

    return bValues.stream().map(bb -> ConversionUtils.<V> asObject(bb)).collect(Collectors.toList());
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<byte[], byte[]>> bbSet = map.readAllEntrySet();

    return bbSet.stream().map(entry -> new AbstractMap.SimpleEntry<>(ConversionUtils.<K> asObject(entry.getKey()), ConversionUtils.<V> asObject(entry.getValue()))).collect(Collectors.toSet());
  }

}
