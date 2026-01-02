package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.time.LocalDate;
import java.util.*;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author Vivek
 * @since 2024-11-11
 */
public interface TickerDataWarehouseService {

  void loadBenchmarkData();

  void loadAnalysisDataForInstruments(Set<String> instruments, boolean isReloadInProgress);

  Set<String> loadAnalysisDataForInstrumentsNotInPortfolio(
      Set<String> instrumentsInPortfolio,
      boolean isReloadInProgress,
      Set<String> imntsNotInPortfolio);

  Double getMarketData(String imnt, int date);

  Double getMarketData(String imnt, LocalDate date);

  boolean containsMarketData(String imnt, LocalDate date);

  List<Pair<LocalDate, LocalDate>> identifyMissingDbDates(
      MarketDataProto.Ticker benchmarkTickerDataFromDb, List<Integer> marketDates);

  LocalDate convertDate(int date);

  List<LocalDate> getDates();

  void fillAnalysisWarehouse(MarketDataProto.Ticker tickerData);
}
