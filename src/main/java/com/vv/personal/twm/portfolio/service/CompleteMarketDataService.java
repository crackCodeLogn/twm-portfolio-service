package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Vivek
 * @since 2025-03-22
 */
public interface CompleteMarketDataService {

  void load();

  void clear();

  double getLatestCombinedCumulativePnL();

  double getLatestTotalInvestmentAmount();

  TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> getCombinedDatePnLCumulativeMap();

  Map<String, Map<MarketDataProto.AccountType, Double>> getCumulativeImntAccountTypeDividendMap();
}
