package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  List<String> getMarketValuations(boolean includeDividends);

  Map<String, String> getMarketValuation(String imnt, MarketDataProto.AccountType accountType);

  Map<String, Double> getAllImntsDividendYieldPercentage();

  Map<String, String> getAllImntsSector();

  Map<String, Double> getNetMarketValuations(
      Optional<MarketDataProto.AccountType> optionalAccountType, boolean includeDividends);

  void setReloadInProgress(boolean reloadInProgress);
}
