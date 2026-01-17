package com.vv.personal.twm.portfolio.cache;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Vivek
 * @since 2026-01-16
 */
public class KeyInstrumentValueCache {
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> keyImntValueCache;

  public KeyInstrumentValueCache() {
    keyImntValueCache = new ConcurrentHashMap<>();
  }

  public void offer(String key, String imnt, Double value) {
    System.out.println(key);
    keyImntValueCache.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
    keyImntValueCache.get(key).put(imnt, value);
  }

  public Optional<Double> get(String key, String symbol) {
    if (keyImntValueCache.containsKey(key))
      return Optional.ofNullable(keyImntValueCache.get(key).get(symbol));
    return Optional.empty();
  }

  public Double remove(String key, String symbol) {
    final Double[] removedVal = new Double[1];
    keyImntValueCache.computeIfPresent(
        key,
        (k, innerMap) -> {
          removedVal[0] = innerMap.remove(symbol);
          return innerMap.isEmpty() ? null : innerMap;
        });
    return removedVal[0];
  }

  public boolean containsKey(String key) {
    return keyImntValueCache.containsKey(key);
  }

  public boolean containsImntForKey(String key, String imnt) {
    return containsKey(key) && keyImntValueCache.get(key).containsKey(imnt);
  }

  public void flushKey(String key) {
    if (containsKey(key)) keyImntValueCache.remove(key);
  }

  public void flushAll() {
    keyImntValueCache.clear();
  }
}
