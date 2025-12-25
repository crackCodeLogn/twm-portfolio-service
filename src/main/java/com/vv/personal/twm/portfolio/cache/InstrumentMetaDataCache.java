package com.vv.personal.twm.portfolio.cache;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Vivek
 * @since 2025-12-21
 */
public class InstrumentMetaDataCache {
  private final ConcurrentHashMap<String, MarketDataProto.Instrument> imntMetaDataCache;

  public InstrumentMetaDataCache() {
    imntMetaDataCache = new ConcurrentHashMap<>();
  }

  public void offer(MarketDataProto.Instrument instrument) {
    imntMetaDataCache.put(instrument.getTicker().getSymbol(), instrument);
  }

  public Optional<MarketDataProto.Instrument> get(String symbol) {
    return Optional.ofNullable(imntMetaDataCache.get(symbol));
  }

  public MarketDataProto.Instrument remove(String symbol) {
    return imntMetaDataCache.remove(symbol);
  }

  public boolean contains(String symbol) {
    return imntMetaDataCache.containsKey(symbol);
  }

  public Set<String> getAllInstruments() {
    return imntMetaDataCache.keySet();
  }

  public void flush() {
    imntMetaDataCache.clear();
  }
}
