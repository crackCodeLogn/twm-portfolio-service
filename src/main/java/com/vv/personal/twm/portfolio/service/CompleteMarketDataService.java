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

  Map<String, Double> getSectorLevelAggrDataMap(MarketDataProto.AccountType accountType);

  Map<String, String> getSectorLevelImntAggrDataMap(MarketDataProto.AccountType accountType);

  Map<String, String> getBestAndWorstPerformers(
      MarketDataProto.AccountType accountType, int n, boolean includeDividends);

  Map<String, String> getMarketValuation(String imnt, MarketDataProto.AccountType accountType);

  Map<String, Double> getAllImntsDividendYieldPercentage();

  Map<String, String> getAllImntsSector();
}
