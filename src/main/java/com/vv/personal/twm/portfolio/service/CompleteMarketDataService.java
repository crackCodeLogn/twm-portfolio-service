package com.vv.personal.twm.portfolio.service;

/**
 * @author Vivek
 * @since 2025-03-22
 */
public interface CompleteMarketDataService {

  void load();

  void clear();

  double getLatestCombinedCumulativePnL();

  double getLatestTotalInvestmentAmount();
}
