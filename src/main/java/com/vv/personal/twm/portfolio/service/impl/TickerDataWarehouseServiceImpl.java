package com.vv.personal.twm.portfolio.service.impl;

import com.google.common.collect.Sets;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.TickerDataWarehouseConfig;
import com.vv.personal.twm.portfolio.market.warehouse.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import java.time.LocalDate;
import java.util.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2024-11-11
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TickerDataWarehouseServiceImpl implements TickerDataWarehouseService {
  private final TickerDataWarehouseConfig tickerDataWarehouseConfig;
  private final MarketDataPythonEngineFeign marketDataPythonEngineFeign;
  private final MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;
  private final TickerDataWarehouse tickerDataWarehouse;

  private final List<Integer> marketDates = new ArrayList<>(100000);
  private final LocalDate endDate = LocalDate.now().plusDays(1);

  @Override
  public void loadBenchmarkData() {
    String benchmarkTicker = tickerDataWarehouseConfig.getBenchmarkTicker();
    LocalDate lookBackDate =
        LocalDate.now().minusYears(tickerDataWarehouseConfig.getLookBackYears());

    MarketDataProto.Ticker benchmarkTickerData =
        marketDataPythonEngineFeign.getTickerDataWithoutCountryCode(
            benchmarkTicker, lookBackDate.toString(), endDate.toString());
    populateMarketDates(benchmarkTickerData);

    loadAnalysisDataForInstruments(Sets.newHashSet(benchmarkTicker));
  }

  @Override
  public void loadAnalysisDataForInstruments(Set<String> instruments) {
    instruments.forEach( // don't parallelize just yet due to py flask
        instrument -> {
          log.info("Loading analysis data for {}", instrument);
          MarketDataProto.Ticker tickerDataFromDb =
              marketDataCrdbServiceFeign.getMarketDataByTicker(instrument);
          fillAnalysisWarehouse(tickerDataFromDb);

          List<Pair<LocalDate, LocalDate>> missingDbDataDates =
              identifyMissingDbDates(tickerDataFromDb, marketDates);
          missingDbDataDates.forEach(
              dates -> {
                log.info(
                    "Downloading missing data for {} from {} -> {}",
                    instrument,
                    dates.getLeft(),
                    dates.getRight());
                MarketDataProto.Ticker missingTickerDataRange =
                    marketDataPythonEngineFeign.getTickerDataWithoutCountryCode(
                        instrument, dates.getLeft().toString(), dates.getRight().toString());

                fillAnalysisWarehouse(missingTickerDataRange);
                log.info(
                    "Adding market data to db for {} from {} -> {}",
                    instrument,
                    dates.getLeft(),
                    dates.getRight());
                marketDataCrdbServiceFeign.addMarketDataForSingleTicker(missingTickerDataRange);
              });
        });
  }

  @Override
  public Double getMarketData(String imnt, int date) {
    return tickerDataWarehouse.get(convertDate(date), imnt);
  }

  @Override
  public List<Pair<LocalDate, LocalDate>> identifyMissingDbDates(
      MarketDataProto.Ticker benchmarkTickerDataFromDb, List<Integer> marketDates) {
    Set<Integer> dates = new HashSet<>();
    if (benchmarkTickerDataFromDb != null)
      benchmarkTickerDataFromDb.getDataList().forEach(value -> dates.add(value.getDate()));
    Integer startDate = -1;
    List<Pair<LocalDate, LocalDate>> targetRanges = new ArrayList<>();
    int date = 0;

    for (Integer marketDate : marketDates) {
      date = marketDate;

      if (!dates.contains(date)) {
        if (startDate == -1) startDate = date;

      } else if (startDate != -1) {
        targetRanges.add(Pair.of(convertDate(startDate), convertDate(date)));
        startDate = -1;
      }
    }
    if (startDate != -1) targetRanges.add(Pair.of(convertDate(startDate), convertDate(date)));

    for (int i = 0; i < targetRanges.size(); i++) { // handle any same left and right days
      Pair<LocalDate, LocalDate> targetRange = targetRanges.get(i);
      if (targetRange.getRight().equals(targetRange.getLeft()))
        targetRanges.set(i, Pair.of(targetRange.getLeft(), targetRange.getRight().plusDays(1)));
    }
    return targetRanges;
  }

  @Override
  public LocalDate convertDate(int date) {
    int year = date / 10000;
    date %= 10000;
    int month = date / 100;
    date %= 100;
    return LocalDate.of(year, month, date);
  }

  private void fillAnalysisWarehouse(MarketDataProto.Ticker tickerData) {
    if (tickerData == null) return;

    String ticker = tickerData.getSymbol();
    tickerData
        .getDataList()
        .forEach(
            value -> {
              LocalDate date = DateFormatUtil.getLocalDate(value.getDate());
              tickerDataWarehouse.put(date, ticker, value.getPrice());
            });
  }

  private void populateMarketDates(MarketDataProto.Ticker benchmarkTickerData) {
    benchmarkTickerData.getDataList().forEach(value -> marketDates.add(value.getDate()));
    Collections.sort(marketDates);
  }

  /*
  public void generateTickerDataWarehouse(PortfolioData portfolioData) {
    LocalDate firstStartDate = LocalDate.of(2999, 12, 31);
    TreeSet<String> instruments = new TreeSet<>();

    for (MarketDataProto.Instrument instrument :
        portfolioData.getPortfolio().getInstrumentsList()) {
      instruments.add(instrument.getTicker().getSymbol());

      LocalDate date = DateFormatUtil.getLocalDate(instrument.getTicker().getData(0).getDate());
      if (date.isBefore(firstStartDate)) {
        firstStartDate = date;
      }
    }
    LocalDate endDate = LocalDate.now().plusDays(1);
    LocalDate startDateForAnalysis = endDate.minusYears(7);

    if (tickerDataWarehouseConfig.isLoad()) {
      generateData(instruments, startDateForAnalysis, endDate);
    }

    // warehouse.setPortfolioData(portfolioData);
    // return warehouse;
  }

  public void generateData(
      TreeSet<String> instruments, LocalDate startDateForAnalysis, LocalDate endDate) {
    instruments.forEach(
        imnt -> {
          log.info("Downloading data for {} from {} to {}", imnt, startDateOfInvestment, endDate);
          MarketDataProto.Ticker tickerData =
              marketDataPythonEngineFeign.getTickerDataWithoutCountryCode(
                  imnt, startDateForAnalysis.toString(), endDate.toString());

          tickerData
              .getDataList()
              .forEach(
                  data -> {
                    LocalDate date = DateFormatUtil.getLocalDate(data.getDate());
                    adjustedClosePriceTableForAnalysis.put(date, imnt, data.getPrice());
                    if (!date.isBefore(startDateOfInvestment))
                      adjustedClosePriceTable.put(date, imnt, data.getPrice());
                  });
        });
    log.debug(adjustedClosePriceTable.toString());
  }*/

}
