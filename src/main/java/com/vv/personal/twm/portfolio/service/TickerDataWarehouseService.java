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

  void loadAnalysisDataForInstruments(Set<String> instruments);

  Double getMarketData(String imnt, int date);

  List<Pair<LocalDate, LocalDate>> identifyMissingDbDates(
      MarketDataProto.Ticker benchmarkTickerDataFromDb, List<Integer> marketDates);

  LocalDate convertDate(int date);

  List<LocalDate> getDates();
}
