package com.vv.personal.twm.portfolio.cache;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import java.time.LocalDate;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * @author Vivek
 * @since 2025-01-12
 */
public class DateLocalDateCache {

  private final BiMap<Integer, LocalDate> dateLocalDateCache;

  public DateLocalDateCache() {
    this.dateLocalDateCache = HashBiMap.create();
  }

  public void add(int date) {
    dateLocalDateCache.putIfAbsent(date, DateFormatUtil.getLocalDate(date));
  }

  public void add(LocalDate date) {
    if (!contains(date)) dateLocalDateCache.put(DateFormatUtil.getDate(date), date);
  }

  public Optional<LocalDate> get(int date) {
    return Optional.ofNullable(dateLocalDateCache.get(date));
  }

  public OptionalInt get(LocalDate date) {
    return OptionalInt.of(dateLocalDateCache.inverse().get(date));
  }

  public boolean contains(int date) {
    return dateLocalDateCache.containsKey(date);
  }

  public boolean contains(LocalDate date) {
    return dateLocalDateCache.inverse().containsKey(date);
  }

  public void flush() {
    dateLocalDateCache.clear();
  }
}
