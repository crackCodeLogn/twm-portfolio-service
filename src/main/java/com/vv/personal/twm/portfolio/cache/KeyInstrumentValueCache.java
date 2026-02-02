package com.vv.personal.twm.portfolio.cache;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Vivek
 * @since 2026-01-16
 */
public class KeyInstrumentValueCache {
  private final ConcurrentHashMap<String, Record> keyImntValueCache;

  public KeyInstrumentValueCache() {
    keyImntValueCache = new ConcurrentHashMap<>();
  }

  public void setFlushForKey(String key, boolean flush) {
    if (!keyImntValueCache.containsKey(key)) {
      keyImntValueCache.put(key, new Record(flush, new ConcurrentHashMap<>()));
    }
  }

  public void offer(String key, String imnt, Double value) {
    if (!keyImntValueCache.containsKey(key)) setFlushForKey(key, true);
    keyImntValueCache.get(key).imntValueMap().put(imnt, value);
  }

  public Optional<Double> get(String key, String symbol) {
    if (keyImntValueCache.containsKey(key))
      return Optional.ofNullable(keyImntValueCache.get(key).imntValueMap().get(symbol));
    return Optional.empty();
  }

  public Double remove(String key, String symbol) {
    final Double[] removedVal = new Double[1];
    keyImntValueCache.computeIfPresent(
        key,
        (k, record) -> {
          removedVal[0] = record.imntValueMap().remove(symbol);
          return record.imntValueMap().isEmpty() ? null : record;
        });
    return removedVal[0];
  }

  public boolean containsKey(String key) {
    return keyImntValueCache.containsKey(key);
  }

  public boolean containsImntForKey(String key, String imnt) {
    return containsKey(key) && keyImntValueCache.get(key).imntValueMap().containsKey(imnt);
  }

  public void flushKey(String key) {
    if (containsKey(key)) keyImntValueCache.remove(key);
  }

  public void flushAll() {
    Set<String> keysToDel = new HashSet<>();
    keyImntValueCache.keySet().stream()
        .filter(key -> keyImntValueCache.get(key).flush())
        .forEach(keysToDel::add);
    keysToDel.forEach(keyImntValueCache::remove);
  }

  private record Record(boolean flush, ConcurrentHashMap<String, Double> imntValueMap) {}
}
