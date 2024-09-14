package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;

/**
 * @author Vivek
 * @since 2024-09-13
 */
@Getter
public class CompleteMarketData {

  private final Map<String, Map<MarketDataProto.AccountType, DataList>> marketData;

  public CompleteMarketData() {
    marketData = new ConcurrentHashMap<>();
  }

  public void populate(MarketDataProto.Portfolio portfolio) {
    portfolio
        .getInstrumentsList()
        .forEach(
            instrument -> {
              marketData.computeIfAbsent(instrument.getTicker().getSymbol(), k -> new HashMap<>());

              Map<MarketDataProto.AccountType, DataList> dataListMap =
                  marketData.get(instrument.getTicker().getSymbol());
              dataListMap.computeIfAbsent(instrument.getAccountType(), k -> new DataList());

              dataListMap.get(instrument.getAccountType()).addBlock(instrument);
            });

    // computing ACB
    marketData.values().forEach(collection -> collection.values().forEach(DataList::computeAcb));
  }
}
