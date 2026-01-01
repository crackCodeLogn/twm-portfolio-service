package com.vv.personal.twm.portfolio.service.impl;

import com.google.common.collect.Sets;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.TickerDataWarehouseConfig;
import com.vv.personal.twm.portfolio.model.market.OutdatedSymbol;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market.outdated.OutdatedSymbols;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.warehouse.market.TickerDataWarehouse;
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
  private final OutdatedSymbols outdatedSymbols;

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

    loadAnalysisDataForInstruments(Sets.newHashSet(benchmarkTicker), false);
  }

  @Override
  public void loadAnalysisDataForInstruments(Set<String> instruments, boolean isReloadInProgress) {
    instruments.forEach( // don't parallelize just yet due to py flask
        instrument -> {
          log.info("Loading analysis data for {}", instrument);
          if (isReloadInProgress) {
            int currentDateForMarketDataRemoval = DateFormatUtil.getDate(LocalDate.now());
            log.info(
                "Forcing removal of market data for {} x {}",
                instrument,
                currentDateForMarketDataRemoval);

            marketDataCrdbServiceFeign.deleteMarketData(
                instrument, currentDateForMarketDataRemoval);
          }

          MarketDataProto.Ticker tickerDataFromDb =
              marketDataCrdbServiceFeign.getMarketDataByTicker(instrument);
          fillAnalysisWarehouse(tickerDataFromDb);

          List<Pair<LocalDate, LocalDate>> missingDbDataDates =
              identifyMissingDbDates(tickerDataFromDb, marketDates);
          missingDbDataDates.forEach(
              inputDates -> {
                List<Pair<LocalDate, LocalDate>> dateList = new ArrayList<>();
                if (outdatedSymbols.contains(instrument)) {
                  dateList =
                      identifyMissingDatesDueToOutdated(
                          outdatedSymbols, instrument, inputDates, marketDates);

                  if (dateList.isEmpty()) {
                    log.info(
                        "Skipping outdated symbol {} from {} to {}",
                        instrument,
                        inputDates.getLeft(),
                        inputDates.getRight());
                  }
                } else { // imnt not in outdated list
                  dateList.add(inputDates);
                }
                for (Pair<LocalDate, LocalDate> dates : dateList) {
                  log.info(
                      "Downloading missing data for {} from {} -> {}",
                      instrument,
                      dates.getLeft(),
                      dates.getRight());
                  MarketDataProto.Ticker missingTickerDataRange =
                      marketDataPythonEngineFeign.getTickerDataWithoutCountryCode(
                          instrument, dates.getLeft().toString(), dates.getRight().toString());

                  if (missingTickerDataRange == null
                      || missingTickerDataRange.getDataCount() == 0) {
                    log.warn(
                        "No data found for {} from {} -> {}",
                        instrument,
                        dates.getLeft(),
                        dates.getRight());
                  } else {
                    fillAnalysisWarehouse(missingTickerDataRange);
                    log.info(
                        "Adding market data to db for {} from {} -> {}",
                        instrument,
                        dates.getLeft(),
                        dates.getRight());
                    marketDataCrdbServiceFeign.addMarketDataForSingleTicker(missingTickerDataRange);
                  }
                }
              });
        });
  }

  @Override
  public Set<String> loadAnalysisDataForInstrumentsNotInPortfolio(
      Set<String> instrumentsInPortfolio,
      boolean isReloadInProgress,
      Set<String> imntsNotInPortfolio) {
    Set<String> allUniqueInstruments = new HashSet<>();
    try {
      List<String> allUniqueTickers = marketDataCrdbServiceFeign.getAllUniqueTickers();
      allUniqueInstruments = new HashSet<>(allUniqueTickers);
    } catch (Exception e) {
      log.error("Failed to load analysis data for instruments not in portfolio", e);
    }

    if (allUniqueInstruments.isEmpty()) {
      log.warn(
          "No unique tickers found for instruments in portfolio, which is a bad state, cannot proceed");
    } else {
      allUniqueInstruments.removeAll(instrumentsInPortfolio);
      allUniqueInstruments.addAll(
          imntsNotInPortfolio); // forcing a reload for the initially not in portf imnts
      if (!allUniqueInstruments.isEmpty()) {
        log.info(
            "Loading analysis data for {} instruments not in portfolio",
            allUniqueInstruments.size());
        loadAnalysisDataForInstruments(allUniqueInstruments, isReloadInProgress);
      }
    }
    return allUniqueInstruments;
  }

  List<Pair<LocalDate, LocalDate>> identifyMissingDatesDueToOutdated(
      OutdatedSymbols outdatedSymbols,
      String imnt,
      Pair<LocalDate, LocalDate> inputDates,
      List<Integer> marketDates) {
    if (outdatedSymbols.get(imnt).isEmpty()) return Collections.emptyList();

    if (outdatedSymbols.get(imnt).get().size() == 1)
      return identifyMissingDatesDueToOutdated(outdatedSymbols.get(imnt).get().first(), inputDates);

    Optional<Integer> startDateIndex =
        locateIndexInMarketDates(inputDates.getLeft(), marketDates, true);
    if (startDateIndex.isEmpty()) {
      log.warn(
          "Could not find start date [{}] for corresponding index in market dates",
          inputDates.getLeft());
      return Collections.emptyList();
    }
    Optional<Integer> endDateIndex =
        locateIndexInMarketDates(inputDates.getRight(), marketDates, false);
    if (endDateIndex.isEmpty()) {
      log.warn(
          "Could not find end date [{}] for corresponding index in market dates",
          inputDates.getLeft());
      return Collections.emptyList();
    }

    List<Pair<LocalDate, LocalDate>> missingDateList = new ArrayList<>();
    LocalDate placeholder = null;
    for (int i = startDateIndex.get(); i <= endDateIndex.get(); i++) {
      int date = marketDates.get(i);

      if (!outdatedSymbols.isCurrentDateOutdated(imnt, date)) { // found a genuine missing date
        if (placeholder == null) {
          placeholder = DateFormatUtil.getLocalDate(date);
        }
      } else {
        if (placeholder != null) {
          missingDateList.add(Pair.of(placeholder, DateFormatUtil.getLocalDate(date)));
          placeholder = null;
        }
      }
    }
    if (placeholder != null) {
      missingDateList.add(
          Pair.of(
              placeholder,
              DateFormatUtil.getLocalDate(marketDates.get(endDateIndex.get())).plusDays(1)));
    }
    return missingDateList;
  }

  /** assumption: marketDates is sorted, log(n) look up */
  Optional<Integer> locateIndexInMarketDates(
      LocalDate localDate, List<Integer> marketDates, boolean forwardSearch) {
    Optional<Integer> targetIndex = Optional.empty();
    int counter = forwardSearch ? 1 : -1;
    int range = 11;

    while (range-- > 0) {
      int date = DateFormatUtil.getDate(localDate);
      int searchResult = Collections.binarySearch(marketDates, date);
      if (searchResult >= 0) return Optional.of(searchResult);
      localDate = localDate.plusDays(counter);
    }
    return targetIndex;
  }

  List<Pair<LocalDate, LocalDate>> identifyMissingDatesDueToOutdated(
      OutdatedSymbol outdatedSymbol, Pair<LocalDate, LocalDate> inputDates) {
    List<Pair<LocalDate, LocalDate>> missingDateList = new ArrayList<>();
    int odStart = outdatedSymbol.outdateStartDate(), odEnd = outdatedSymbol.outdateEndDate();
    int start = DateFormatUtil.getDate(inputDates.getLeft()),
        end = DateFormatUtil.getDate(inputDates.getRight());

    // 11 cases handled
    if (start >= odStart && end <= odEnd) { // date range lies in complete outdate range
      return missingDateList;
    } else if (start < odStart && end > odEnd) {
      missingDateList.add(Pair.of(inputDates.getLeft(), DateFormatUtil.getLocalDate(odStart)));
      missingDateList.add(
          Pair.of(DateFormatUtil.getLocalDate(odEnd).plusDays(1), inputDates.getRight()));
    } else if (start == odEnd) {
      missingDateList.add(
          Pair.of(DateFormatUtil.getLocalDate(odEnd).plusDays(1), inputDates.getRight()));
    } else if (end <= odStart || start >= odEnd) { // current date range outside outdate range
      missingDateList.add(inputDates);
    } else if (start < odStart) {
      missingDateList.add(Pair.of(inputDates.getLeft(), DateFormatUtil.getLocalDate(odStart)));
    }

    return missingDateList;
  }

  @Override
  public Double getMarketData(String imnt, int date) {
    return tickerDataWarehouse.get(convertDate(date), imnt);
  }

  @Override
  public boolean containsMarketData(String imnt, LocalDate date) {
    return tickerDataWarehouse.contains(date, imnt);
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

    for (int i = 0;
        i < targetRanges.size() - 1;
        i++) { // handle any same left and right days, except last
      Pair<LocalDate, LocalDate> targetRange = targetRanges.get(i);
      if (targetRange.getRight().equals(targetRange.getLeft()))
        targetRanges.set(i, Pair.of(targetRange.getLeft(), targetRange.getRight().plusDays(1)));
    }
    if (!targetRanges.isEmpty() && !dates.contains(marketDates.get(marketDates.size() - 1))) {
      int lastIndex = targetRanges.size() - 1;
      targetRanges.set(
          lastIndex,
          Pair.of(
              targetRanges.get(lastIndex).getLeft(),
              targetRanges.get(lastIndex).getRight().plusDays(1)));
    }
    return targetRanges;
  }

  @Override
  public LocalDate convertDate(int date) {
    // return DateFormatUtil.getLocalDate(date); // can be used as an alternate as well, but may be
    // less optimal? test it out

    int year = date / 10000;
    date %= 10000;
    int month = date / 100;
    date %= 100;
    return LocalDate.of(year, month, date);
  }

  @Override
  public List<LocalDate> getDates() {
    return tickerDataWarehouse.getDates();
  }

  @Override
  public void fillAnalysisWarehouse(MarketDataProto.Ticker tickerData) {
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
