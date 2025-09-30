package com.vv.personal.twm.portfolio.service.impl;

import static com.vv.personal.twm.portfolio.util.SanitizerUtil.sanitizeAndFormat2Double;
import static com.vv.personal.twm.portfolio.util.SanitizerUtil.sanitizeDouble;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.model.market.DataList;
import com.vv.personal.twm.portfolio.model.market.DataNode;
import com.vv.personal.twm.portfolio.model.market.DividendRecord;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market.outdated.OutdatedSymbols;
import com.vv.personal.twm.portfolio.service.CompleteMarketDataService;
import com.vv.personal.twm.portfolio.service.ExtractMarketPortfolioDataService;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.util.SanitizerUtil;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2024-09-13
 */
@Slf4j
@Getter
@Setter
@Service
public class CompleteMarketDataServiceImpl implements CompleteMarketDataService {
  private static final int TODAY_DATE = DateFormatUtil.getDate(LocalDate.now());
  private static final String UNKNOWN_SECTOR = "UNKNOWN";

  // Holds map of ticker x (map of account type x doubly linked list nodes of transactions done)
  private final Map<String, Map<MarketDataProto.AccountType, DataList>> marketData;
  private final Map<String, Map<MarketDataProto.AccountType, Map<Integer, List<DividendRecord>>>>
      imntDividendsMap;
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      dateDividendsMap; // date x account type x divs for that date

  // post processes, i.e. not filled during startup
  // todo - think about filling all the date based maps with 0s based off entire dates
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      realizedDatePnLMap; // pure date x account type x sells
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      unrealizedDatePnLMap; // pure date x account type x unrealized
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      combinedDatePnLMap; // combined pure date x account type x (realized + unrealized)
  private final Map<String, Map<MarketDataProto.AccountType, TreeMap<Integer, Double>>>
      unrealizedImntPnLMap; // pure imnt x account type x date x unrealized
  private final Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>>
      realizedImntPnLMap; // pure imnt sells
  private final TreeMap<Integer, Map<MarketDataProto.AccountType, Double>>
      realizedWithDividendDatePnLMap; // cumulative date x account type x (sells + divs)
  private final Map<String, Map<MarketDataProto.AccountType, TreeMap<Integer, Double>>>
      realizedImntWithDividendPnLMap; // cumulative imnt x account type x date x (sells + divs)
  private final TreeMap<Integer, Map<MarketDataProto.AccountType, Double>>
      dateDividendsCumulativeMap; // cumulative date x account type x divs
  private final TreeMap<Integer, Map<MarketDataProto.AccountType, Double>>
      combinedDatePnLCumulativeMap; // cumulative date x account type x (unrealized + sells + divs)
  // SPECIAL NOTE: combinedDatePnLCumulativeMap does not include div-only non-market dates
  private final Map<String, Map<MarketDataProto.AccountType, Double>> cumulativeImntDividendsMap;
  private final Map<String, Map<MarketDataProto.AccountType, Map<String, Double>>>
      sectorLevelImntAggrMap; // sector level x account type x (imnt x investment-imnt)
  private final Map<String, String> imntSectorMap; // imnt x sector
  private final Map<String, Double> imntDivYieldMap; // imnt x div yield %

  private final DateLocalDateCache dateLocalDateCache;
  private final ExtractMarketPortfolioDataService extractMarketPortfolioDataService;
  private final TickerDataWarehouseService tickerDataWarehouseService;
  private final MarketDataPythonEngineFeign marketDataPythonEngineFeign;
  private OutdatedSymbols outdatedSymbols;

  public CompleteMarketDataServiceImpl(
      DateLocalDateCache dateLocalDateCache,
      ExtractMarketPortfolioDataService extractMarketPortfolioDataService,
      TickerDataWarehouseService tickerDataWarehouseService,
      MarketDataPythonEngineFeign marketDataPythonEngineFeign) {
    marketData = new ConcurrentHashMap<>();
    imntDividendsMap = new ConcurrentHashMap<>();
    dateDividendsMap = new ConcurrentHashMap<>();
    realizedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    unrealizedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    combinedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    unrealizedImntPnLMap = new ConcurrentHashMap<>();
    realizedImntPnLMap = new ConcurrentHashMap<>();
    realizedWithDividendDatePnLMap = new TreeMap<>();
    realizedImntWithDividendPnLMap = new ConcurrentHashMap<>();
    dateDividendsCumulativeMap = new TreeMap<>();
    combinedDatePnLCumulativeMap = new TreeMap<>();
    cumulativeImntDividendsMap = new ConcurrentHashMap<>();
    sectorLevelImntAggrMap = new ConcurrentHashMap<>();
    imntSectorMap = new ConcurrentHashMap<>();
    imntDivYieldMap = new ConcurrentHashMap<>();

    this.tickerDataWarehouseService = tickerDataWarehouseService;
    this.dateLocalDateCache = dateLocalDateCache;
    this.extractMarketPortfolioDataService = extractMarketPortfolioDataService;
    this.marketDataPythonEngineFeign = marketDataPythonEngineFeign;
  }

  @Override
  public void load() {
    log.info("Initiating complete market data load");
    StopWatch stopWatch = StopWatch.createStarted();

    // first populate the buy side
    populate(
        extractMarketPortfolioDataService
            .extractMarketPortfolioData(MarketDataProto.Direction.BUY)
            .getPortfolio());
    // then populate the sell side
    populate(
        extractMarketPortfolioDataService
            .extractMarketPortfolioData(MarketDataProto.Direction.SELL)
            .getPortfolio());
    computeAcb(); // compute the ACB once all the data has been populated

    populateDividends(
        extractMarketPortfolioDataService
            .extractMarketPortfolioDividendData(MarketDataProto.AccountType.TFSA)
            .getPortfolio());
    populateDividends(
        extractMarketPortfolioDataService
            .extractMarketPortfolioDividendData(MarketDataProto.AccountType.NR)
            .getPortfolio());
    populateDividends(
        extractMarketPortfolioDataService
            .extractMarketPortfolioDividendData(MarketDataProto.AccountType.FHSA)
            .getPortfolio());

    // load analysis data for imnts which are bought
    tickerDataWarehouseService.loadAnalysisDataForInstruments(getInstruments());
    computePnL();

    computeCumulativeDividend();

    computeSectorLevelImntAggregationData();

    populateDividendYields();

    stopWatch.stop();
    log.info(
        "Complete market data load finished in {}ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  private void populateDividendYields() {
    log.info("Populating dividend yield data for all imnts");
    marketData
        .keySet()
        .forEach(
            imnt -> {
              log.info("Querying dividend yield for imnt {}", imnt);
              String divYield = marketDataPythonEngineFeign.getTickerDividend(imnt);

              if (StringUtils.isEmpty(divYield)) {
                log.warn("No dividend yield data found for {}", imnt);
              } else if (NumberUtils.isParsable(divYield)) {
                imntDivYieldMap.put(imnt, Double.parseDouble(divYield));
              } else {
                log.warn("Unable to parse div yield data for {} => {}", imnt, divYield);
              }
            });
  }

  void computeSectorLevelImntAggregationData() {
    log.info(
        "Initiating compute of sector level imnt aggregation data, using live sector check calls");

    for (Map.Entry<String, Map<MarketDataProto.AccountType, DataList>> entry :
        marketData.entrySet()) {
      String imnt = entry.getKey();
      Map<MarketDataProto.AccountType, DataList> accountTypeDataListMap = entry.getValue();
      log.info("Computing live sector and imnt aggr compute for {}", imnt);

      // commented for now, as yfinance sector info is diff from codes i am using
      /*String sectorResponse =
          imntSectorMap.getOrDefault(
              imnt,
              SanitizerUtil.sanitizeSector(marketDataPythonEngineFeign.getTickerSector(imnt)));
      if (StringUtils.isEmpty(sectorResponse) || "unknown".equals(sectorResponse)) {
        log.warn("Could not determine sector for imnt {}", imnt);
        sectorResponse = UNKNOWN_SECTOR;
      }
      imntSectorMap.putIfAbsent(imnt, sectorResponse);*/

      accountTypeDataListMap.forEach(
          (accountType, dataList) -> {
            DataNode node = dataList.getTail();
            if (node == null) {
              log.error("Found a null node in the datalist of {} x {}", imnt, accountType);
            } else if (node.getInstrument() == null || node.getInstrument().getTicker() == null) {
              log.error(
                  "Found a null node instrument in the datalist of {} x {}", imnt, accountType);
            } else {
              // commented for now, as yfinance sector info is diff from codes i am using
              /*if (imntSectorMap.get(imnt).equals(UNKNOWN_SECTOR)) {
                String overrideSector =
                    SanitizerUtil.sanitizeSector(node.getInstrument().getTicker().getSector());
                log.warn("Overriding imnt {} sector to {}", imnt, overrideSector);
                imntSectorMap.put(imnt, overrideSector);
              }
              String sector = imntSectorMap.get(imnt);*/
              String sector =
                  SanitizerUtil.sanitizeSector(node.getInstrument().getTicker().getSector());
              imntSectorMap.put(imnt, sector);

              // create placeholder for sector x account type x imnt
              sectorLevelImntAggrMap
                  .computeIfAbsent(sector, k -> new ConcurrentHashMap<>())
                  .computeIfAbsent(accountType, k -> new ConcurrentHashMap<>())
                  .computeIfAbsent(imnt, k -> 0.0);

              double investment = node.getAcb().getTotalAcb();
              sectorLevelImntAggrMap.get(sector).get(accountType).put(imnt, investment);
            }
          });
    }

    log.info(
        "Completed calculation of sector level instrument aggregation data, with a total of {} sectors",
        sectorLevelImntAggrMap.size());
  }

  private void computeCumulativeDividend() {
    imntDividendsMap.forEach(
        (imnt, accountTypeMap) ->
            accountTypeMap.forEach(
                (accountType, dateDividendMap) -> {
                  double sum = 0;
                  for (List<DividendRecord> dividendRecords : dateDividendMap.values()) {
                    for (DividendRecord dividendRecord : dividendRecords) {
                      sum += dividendRecord.dividend();
                    }
                  }
                  cumulativeImntDividendsMap
                      .compute(imnt, (k, v) -> v == null ? new HashMap<>() : v)
                      .put(accountType, sum);
                }));
    log.info("Completed calculating sum of dividends per imnt and account type");
  }

  @Override
  public void clear() {
    log.warn("Initiating complete market data clearing");
    marketData.clear();
    imntDividendsMap.clear();
    dateDividendsMap.clear();
    realizedDatePnLMap.clear();
    unrealizedDatePnLMap.clear();
    combinedDatePnLMap.clear();
    unrealizedImntPnLMap.clear();
    realizedImntPnLMap.clear();
    realizedWithDividendDatePnLMap.clear();
    realizedImntWithDividendPnLMap.clear();
    dateDividendsCumulativeMap.clear();
    combinedDatePnLCumulativeMap.clear();
    cumulativeImntDividendsMap.clear();
    sectorLevelImntAggrMap.clear();
    imntSectorMap.clear();
    imntDivYieldMap.clear();
    log.info("Completed market data clearing");
  }

  @Override
  public double getLatestCombinedCumulativePnL() {
    double pnl = 0.0;
    Map<MarketDataProto.AccountType, Double> values =
        combinedDatePnLCumulativeMap.floorEntry(TODAY_DATE).getValue();
    for (Double value : values.values()) pnl += value;
    return pnl;
  }

  // todo - write test
  @Override
  public double getLatestTotalInvestmentAmount() {
    double investment = 0.0;
    for (Map<MarketDataProto.AccountType, DataList> entry : marketData.values()) {
      for (DataList dataList : entry.values()) {
        DataNode node = dataList.getHead();
        while (node != null) {
          int multiplier =
              node.getInstrument().getDirection() == MarketDataProto.Direction.SELL ? -1 : 1;
          investment += multiplier * node.getInstrument().getTicker().getData(0).getPrice();
          node = node.getNext();
        }
      }
    }
    System.out.println("total investment: " + investment);
    return investment;
  }

  @Override
  public Map<String, Map<MarketDataProto.AccountType, Double>>
      getCumulativeImntAccountTypeDividendMap() {
    return cumulativeImntDividendsMap;
  }

  @Override
  public Map<String, Double> getSectorLevelAggrDataMap(MarketDataProto.AccountType accountType) {
    Map<String, Double> sectorAggrMap = new HashMap<>();
    sectorLevelImntAggrMap.forEach(
        (sector, accountTypeMap) -> {
          Map<String, Double> imntSumMap = accountTypeMap.get(accountType);
          if (imntSumMap == null) {
            log.debug(
                "Did not find imnt sum map for account type {} sector {}", accountType, sector);
          } else {
            sectorAggrMap.computeIfAbsent(sector, k -> 0.0);
            double sum = 0.0;
            for (Double value : imntSumMap.values()) sum += value;
            sectorAggrMap.put(sector, sectorAggrMap.get(sector) + sum);
          }
        });
    return sectorAggrMap;
  }

  @Override
  public Map<String, String> getSectorLevelImntAggrDataMap(
      MarketDataProto.AccountType accountType) {
    Map<String, StringBuilder> sectorImntAggrMap = new HashMap<>();
    sectorLevelImntAggrMap.forEach(
        (sector, accountTypeMap) -> {
          Map<String, Double> imntSumMap = accountTypeMap.get(accountType);
          if (imntSumMap == null) {
            log.debug(
                "Did not find imnt sum map for account type {} sector {} for getSectorLevelImntAggrDataMap",
                accountType,
                sector);
          } else {
            sectorImntAggrMap.computeIfAbsent(sector, k -> new StringBuilder());
            imntSumMap.forEach(
                (imnt, val) -> {
                  String outputVal = String.format("%s=%.02f|", imnt, val);
                  sectorImntAggrMap.get(sector).append(outputVal);
                });
          }
        });

    Map<String, String> outputMap = new HashMap<>();
    sectorImntAggrMap.forEach(
        (sector, value) -> outputMap.put(sector, value.substring(0, value.length() - 1)));
    sectorImntAggrMap.clear();
    return outputMap;
  }

  @Override
  public Map<String, String> getBestAndWorstPerformers(
      MarketDataProto.AccountType accountType, int n, boolean includeDividends) {
    Map<String, String> bestAndWorstPerformerMap = new HashMap<>();
    Map<String, Pair<Double, Double>> imntPairMap = new HashMap<>();

    unrealizedImntPnLMap.forEach(
        (imnt, accountTypeMap) -> {
          if (accountTypeMap.containsKey(accountType)) {
            Map.Entry<Integer, Double> dateValueEntry =
                accountTypeMap.get(accountType).floorEntry(TODAY_DATE);
            if (dateValueEntry != null) {
              double currentValuationPnL = dateValueEntry.getValue();

              DataNode lastImntNode = marketData.get(imnt).get(accountType).getTail();
              double investmentActual =
                  lastImntNode.getAcb().getAcbPerUnit() * lastImntNode.getRunningQuantity();

              Pair<Double, Double> pair = Pair.of(currentValuationPnL, investmentActual);
              imntPairMap.put(imnt, pair);
            }
          }
        });

    if (includeDividends) {
      log.info("Including dividend and sells in the best and worst performers calculation");
      imntPairMap.forEach(
          (imnt, pair) -> {
            if (realizedImntWithDividendPnLMap.containsKey(imnt)
                && realizedImntWithDividendPnLMap.get(imnt).containsKey(accountType)) {
              double combinedValuation =
                  pair.getRight()
                      + realizedImntWithDividendPnLMap
                          .get(imnt)
                          .get(accountType)
                          .floorEntry(TODAY_DATE)
                          .getValue();
              imntPairMap.put(imnt, Pair.of(combinedValuation, pair.getLeft()));
            }
          });
    }

    List<ImntValuationCurrentPnLAndActual> collectionData = new ArrayList<>(imntPairMap.size());
    for (Map.Entry<String, Pair<Double, Double>> entry : imntPairMap.entrySet()) {
      Double pnlPercentage = (entry.getValue().getLeft() * 100.0 / entry.getValue().getRight());
      if (pnlPercentage != Double.NaN) {
        collectionData.add(
            new ImntValuationCurrentPnLAndActual(
                entry.getKey(),
                entry.getValue().getLeft(),
                entry.getValue().getRight(),
                pnlPercentage));
      } else {
        log.info("Found NaN pnl percentage for {} x {}", entry.getKey(), accountType);
      }
    }

    collectionData.sort(
        Comparator.comparingDouble(ImntValuationCurrentPnLAndActual::pnlPercentage).reversed());

    log.info(
        "Computed {} records for {}, attempting pruning to best {} and worst {} performers",
        collectionData.size(),
        accountType,
        n,
        n);

    if (collectionData.size() > 2 * n) { // prune
      List<ImntValuationCurrentPnLAndActual> tmpList = collectionData.subList(0, n);
      tmpList.addAll(collectionData.subList(collectionData.size() - n, collectionData.size()));
      collectionData = tmpList;
    }

    collectionData.forEach(
        imntValuationCurrentPnLAndActual -> {
          log.info(
              "[{}] {} => {}, {}, {}",
              accountType,
              imntValuationCurrentPnLAndActual.imnt(),
              imntValuationCurrentPnLAndActual.currentValuationPnL(),
              imntValuationCurrentPnLAndActual.investmentActual(),
              imntValuationCurrentPnLAndActual.pnlPercentage());

          bestAndWorstPerformerMap.put(
              imntValuationCurrentPnLAndActual.imnt(),
              String.format(
                  "%.02f|%.02f",
                  imntValuationCurrentPnLAndActual.currentValuationPnL(),
                  imntValuationCurrentPnLAndActual.investmentActual()));
        });

    return bestAndWorstPerformerMap;
  }

  @Override
  public Map<String, String> getMarketValuation(
      String imnt, MarketDataProto.AccountType accountType) {
    Map<String, String> marketValuation = new HashMap<>();

    if (!marketData.containsKey(imnt)) {
      log.warn("No data found in Portfolio for {}", imnt);
      return marketValuation;
    }

    if (marketData.containsKey(imnt) && !marketData.get(imnt).containsKey(accountType)) {
      log.warn("No data found in Portfolio for {} x {}", imnt, accountType);
      return marketValuation;
    }

    DataNode node = marketData.get(imnt).get(accountType).getTail();
    double bookVal = node.getAcb().getAcbPerUnit() * node.getRunningQuantity();
    double pnl = unrealizedImntPnLMap.get(imnt).get(accountType).floorEntry(TODAY_DATE).getValue();
    double currentVal = bookVal + pnl;
    double totalDiv =
        cumulativeImntDividendsMap.containsKey(imnt)
            ? (cumulativeImntDividendsMap.get(imnt).containsKey(accountType)
                ? cumulativeImntDividendsMap.get(imnt).get(accountType)
                : 0.0)
            : 0.0;
    double divYieldPercent = imntDivYieldMap.get(imnt);

    marketValuation.put("imnt", imnt);
    marketValuation.put("accountType", accountType.name());
    marketValuation.put("sector", node.getInstrument().getTicker().getSector());
    marketValuation.put("divYieldPercent", sanitizeAndFormat2Double(divYieldPercent));
    marketValuation.put("bookVal", sanitizeAndFormat2Double(bookVal));
    marketValuation.put("currentVal", sanitizeAndFormat2Double(currentVal));
    marketValuation.put("pnl", sanitizeAndFormat2Double(pnl));
    marketValuation.put("totalDiv", sanitizeAndFormat2Double(totalDiv));

    log.info("Computed market valuation for {} x {}", imnt, accountType);
    return marketValuation;
  }

  @Override
  public Map<String, Double> getAllImntsDividendYieldPercentage() {
    return imntDivYieldMap;
  }

  @Override
  public Map<String, String> getAllImntsSector() {
    return imntSectorMap;
  }

  void populate(MarketDataProto.Portfolio portfolio) {
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
  }

  void populateDividends(MarketDataProto.Portfolio portfolio) {
    dateDividendsCumulativeMap.put(0, new HashMap<>());
    List<MarketDataProto.AccountType> accountTypes = getAccountTypes();
    accountTypes.forEach(type -> dateDividendsCumulativeMap.get(0).put(type, 0.0)); // baseline

    log.info("Beginning dividends population.");
    StopWatch stopWatch = StopWatch.createStarted();

    for (MarketDataProto.Instrument instrument : portfolio.getInstrumentsList()) {
      String orderId = instrument.getMetaDataOrDefault("orderId", "");
      if (orderId.isEmpty()) {
        log.error("orderId is empty for dividend transaction: {}", instrument);
        continue;
      }

      int divDate = instrument.getTicker().getData(0).getDate();
      double dividend = instrument.getTicker().getData(0).getPrice();
      MarketDataProto.AccountType accountType = instrument.getAccountType();

      // record dividend data in imntDividendsMap as the data structure holding translated data from
      // the div list
      imntDividendsMap.computeIfAbsent(instrument.getTicker().getSymbol(), k -> new HashMap<>());
      Map<MarketDataProto.AccountType, Map<Integer, List<DividendRecord>>> divDateValueMap =
          imntDividendsMap.get(instrument.getTicker().getSymbol());
      divDateValueMap.computeIfAbsent(accountType, k -> new HashMap<>());
      divDateValueMap
          .get(accountType)
          .compute(divDate, (k, v) -> v == null ? new ArrayList<>() : v)
          .add(new DividendRecord(instrument.getTicker().getSymbol(), divDate, dividend, orderId));

      // equivalent of above structure but an agg based on date and irrespective of imnt
      Map<MarketDataProto.AccountType, Double> dateDivAccountTypeDivMap =
          dateDividendsMap.computeIfAbsent(divDate, k -> new HashMap<>());
      dateDivAccountTypeDivMap.putIfAbsent(accountType, 0.0);
      dateDivAccountTypeDivMap.compute(accountType, (k, v) -> sanitizeDouble(v) + dividend);
    }

    // operate below for accumulation
    // note: do not add the date / local date to the instance localDateAndDateMap or
    // dateAndLocalDateMap as the absence is used to calculate missing div dates in computePnL
    List<Integer> divDates = new ArrayList<>(dateDividendsMap.keySet());
    Collections.sort(divDates);
    Map<MarketDataProto.AccountType, Double> cumulativeDivs = new HashMap<>();
    accountTypes.forEach(accountType -> cumulativeDivs.put(accountType, 0.0));
    for (int date : divDates) { // skip 0th as that is date = 0
      Map<MarketDataProto.AccountType, Double> typeDivBaseMap = dateDividendsMap.get(date);
      Map<MarketDataProto.AccountType, Double> typeDivTargetMap =
          dateDividendsCumulativeMap.computeIfAbsent(date, k -> new HashMap<>());
      accountTypes.forEach(
          accountType -> typeDivTargetMap.put(accountType, cumulativeDivs.get(accountType)));

      typeDivBaseMap.forEach(
          (type, div) -> {
            typeDivTargetMap.compute(type, (k, v) -> sanitizeDouble(v) + div);
            cumulativeDivs.put(type, cumulativeDivs.getOrDefault(type, 0.0) + div);
          });
    }
    /*
    LocalDate localDate = DateFormatUtil.getLocalDate(divDate);
    int dateTMinus1 = DateFormatUtil.getDate(localDate.minusDays(1));

    Map<MarketDataProto.AccountType, Double> typeDividendCumulativeMap =
        dateDividendsCumulativeMap.computeIfAbsent(divDate, k -> new HashMap<>());
    // update current date with t-1 data point
    accountTypes.forEach(
        accountType1 -> {
          Double divTMinus1AccountTypeDividend =
              dateDividendsCumulativeMap.floorEntry(dateTMinus1).getValue().get(accountType1);
          typeDividendCumulativeMap.putIfAbsent(accountType1, 0.0);
          typeDividendCumulativeMap.compute(
              accountType1, (k, v) -> sanitizeDouble(v) + divTMinus1AccountTypeDividend);
        });

    // update current date with t data point
    typeDividendCumulativeMap.compute(accountType, (k, v) -> sanitizeDouble(v) + dividend);*/

    stopWatch.stop();
    log.info(
        "Dividend calculation and population completed in {} ms",
        stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  void computeAcb() {
    marketData.values().forEach(collection -> collection.values().forEach(DataList::computeAcb));
  }

  void computePnL() {
    List<LocalDate> dates = tickerDataWarehouseService.getDates();
    Set<Integer> localDates = new HashSet<>();
    dates.forEach(
        date -> {
          dateLocalDateCache.add(date);
          localDates.add(dateLocalDateCache.get(date).getAsInt());
        });
    Set<Integer> dividendDates = getDividendDates();
    boolean toSort = false;
    for (Integer dividendDate : dividendDates) {
      if (!localDates.contains(dividendDate)) {
        log.info("Found a non-market dividend date: {}", dividendDate);
        dateLocalDateCache.add(dividendDate);
        dates.add(dateLocalDateCache.get(dividendDate).get());
        toSort = true;
      }
    }
    if (toSort) Collections.sort(dates);

    boolean failed = false;
    outer:
    for (Map.Entry<String, Map<MarketDataProto.AccountType, DataList>> marketDataEntry :
        marketData.entrySet()) { // iterate over the marketData
      String imnt = marketDataEntry.getKey();
      Map<MarketDataProto.AccountType, DataList> typeDataMap = marketDataEntry.getValue();
      for (Map.Entry<MarketDataProto.AccountType, DataList> entry : typeDataMap.entrySet()) {
        MarketDataProto.AccountType type = entry.getKey();
        DataList dataList = entry.getValue();
        DataNode node = dataList.getHead();
        int nodeDate = getDate(node);
        int dateIndex = 0;

        // find the initial date index for the nodeDate
        while (dateIndex < dates.size()
            && dateLocalDateCache.get(dates.get(dateIndex)).getAsInt() != nodeDate) {
          dateIndex++;
        }

        log.info("Found dateIndex: {} for {} of {} {}", dateIndex, nodeDate, imnt, type);
        while (dateIndex < dates.size()) {
          int date = dateLocalDateCache.get(dates.get(dateIndex)).getAsInt();
          // find and point to the correct node for the date
          while (node.getNext() != null && getDate(node.getNext()) == date) {
            node = node.getNext();
          }
          // System.out.println(date + " " + node);

          Double marketPrice = tickerDataWarehouseService.getMarketData(imnt, date);
          Optional<Pair<Double, Integer>> fallbackPriceDate;
          boolean overrideDateIndexIncrementAndContinue = false;
          if (marketPrice == null) {
            if (outdatedSymbols != null && outdatedSymbols.isCurrentDateOutdated(imnt, date)) {
              log.info("Allowing skip of market price for outdated {} x {}", imnt, date);
            } else if (dividendDates.contains(date)) {
              log.info("Allowed to miss off-market dividend date: {}", date);
              dateIndex++;
              continue;
            } else if ((fallbackPriceDate =
                    fetchTMinusPrice(
                        imnt, dateIndex, 7, tickerDataWarehouseService, dates, dateLocalDateCache))
                .isPresent()) {
              log.warn(
                  "Found a backdated price for imnt '{}' at {}: {}",
                  imnt,
                  fallbackPriceDate.get().getRight(),
                  fallbackPriceDate.get().getLeft());
              marketPrice = fallbackPriceDate.get().getLeft();
              overrideDateIndexIncrementAndContinue = true;
            } else {
              log.error(
                  "Did not find market price for {} x {}, CANNOT compute any further!", imnt, date);
              failed = true;
              break outer;
            }
            if (!overrideDateIndexIncrementAndContinue) {
              dateIndex++;
              continue;
            }
          }
          computeUnrealizedPnL(imnt, type, node, date, marketPrice);
          if (node.getInstrument().getDirection() == MarketDataProto.Direction.BUY) {
            dateIndex++;
            continue;
          }
          computeRealizedPnL(
              imnt, type, node, date, marketPrice); // realized pnl (w/o div) changes only on SELL
          // fixed realized pnl mis-calc. rn, because of the last STLC sell node, the dates beyond
          // keep on using the same node and keep showing unnecessary gains whereas the gain was
          // only for 1 day
          // realizedImntPnLMap.get("STLC.TO").get(MarketDataProto.AccountType.TFSA).values().stream().mapToDouble(Double::doubleValue).sum()
          if (node.getNext() == null) {
            log.info("Reached the end of position for {} x {} on {}", imnt, type, date);
            break;
          }
          dateIndex++;
        }
      }
    }
    /*System.out.println("Spewing out acb data list of vfv.to nr to analyze: "); // todo - remove
    DataNode vfvNrNode = marketData.get("VFV.TO").get(MarketDataProto.AccountType.NR).getHead();
    while (vfvNrNode != null) {
      System.out.printf(
          "%s %f %d %f %s %s %f %f %f\n",
          vfvNrNode.getInstrument().getTicker().getSymbol(),
          vfvNrNode.getInstrument().getQty(),
          vfvNrNode.getInstrument().getTicker().getData(0).getDate(),
          vfvNrNode.getInstrument().getTicker().getData(0).getPrice(),
          vfvNrNode.getInstrument().getMetaDataOrDefault("pricePerShare", "0.0"),
          vfvNrNode.getInstrument().getDirection(),
          vfvNrNode.getRunningQuantity(),
          vfvNrNode.getAcb().getAcbPerUnit(),
          vfvNrNode.getAcb().getTotalAcb());
      vfvNrNode = vfvNrNode.getNext();
    }*/

    //    unrealizedDatePnLMap.forEach(
    //        (k, v) -> System.out.printf("%d x %.6f\n", k, v.get(MarketDataProto.AccountType.NR)));

    if (!failed) {
      computeCombinedPnL(dates);

      computeRealizedPnLFromDividends(dates);
      computeCombinedPnLCumulative();
      System.out.println(
          "Combined PnL of TFSA: "
              + combinedDatePnLCumulativeMap
                  .floorEntry(TODAY_DATE)
                  .getValue()
                  .get(MarketDataProto.AccountType.TFSA));
      System.out.println(
          "Combined PnL of NR: "
              + combinedDatePnLCumulativeMap
                  .floorEntry(TODAY_DATE)
                  .getValue()
                  .get(MarketDataProto.AccountType.NR));
      System.out.println(
          "Combined PnL of FHSA: "
              + combinedDatePnLCumulativeMap
                  .floorEntry(TODAY_DATE)
                  .getValue()
                  .get(MarketDataProto.AccountType.FHSA));
    } else log.error("Failed to compute pnL. Check logs for relevant error.");
  }

  private Optional<Pair<Double, Integer>> fetchTMinusPrice(
      String imnt,
      int dateIndex,
      int fallBackDaysLimit,
      TickerDataWarehouseService tickerDataWarehouseService,
      List<LocalDate> dates,
      DateLocalDateCache dateLocalDateCache) {
    for (int tMinusDay = 1; tMinusDay <= fallBackDaysLimit && dateIndex-- > 0; tMinusDay++) {
      int date = dateLocalDateCache.get(dates.get(dateIndex)).getAsInt();
      Double marketPrice = tickerDataWarehouseService.getMarketData(imnt, date);
      if (marketPrice != null) {
        return Optional.ofNullable(Pair.of(marketPrice, date));
      }
    }
    return Optional.empty();
  }

  // inflate realized pnl with dividend data
  private void computeRealizedPnLFromDividends(List<LocalDate> dates) {
    List<MarketDataProto.AccountType> accountTypes = getAccountTypes();
    realizedImntWithDividendPnLMap.clear();
    //    realizedImntWithDividendPnLMap.putAll(realizedImntPnLMap); // doesn't actually create new
    // copies

    // deep copy realizedImntPnLMap -> realizedImntWithDividendPnLMap
    realizedImntPnLMap.forEach(
        (imnt, typeDatePriceMap) ->
            typeDatePriceMap.forEach(
                (type, datePriceMap) ->
                    datePriceMap.forEach(
                        (date, price) -> {
                          Map<MarketDataProto.AccountType, TreeMap<Integer, Double>> imntMap =
                              realizedImntWithDividendPnLMap.computeIfAbsent(
                                  imnt, k -> new HashMap<>());
                          Map<Integer, Double> dtPriceMap =
                              imntMap.computeIfAbsent(type, k -> new TreeMap<>());
                          dtPriceMap.put(date, price);
                        })));
    // deep copy completed

    // populate realizedImntWithDividendPnLMap
    for (String dividendTicker : imntDividendsMap.keySet()) {
      Map<MarketDataProto.AccountType, Map<Integer, List<DividendRecord>>> accountTypeMapMap =
          imntDividendsMap.get(dividendTicker);

      accountTypeMapMap.forEach(
          (accountType, dateRecordMap) ->
              dateRecordMap.forEach(
                  (date, records) -> {
                    // init
                    Map<MarketDataProto.AccountType, TreeMap<Integer, Double>> typeMapMap =
                        realizedImntWithDividendPnLMap.computeIfAbsent(
                            dividendTicker,
                            k2 -> new HashMap<>()); // for CAD manufactured kind of things
                    typeMapMap.computeIfAbsent(accountType, k1 -> new TreeMap<>());
                    typeMapMap.get(accountType).putIfAbsent(0, 0.0); // baseline

                    int dateTMinus1 =
                        DateFormatUtil.getDate(dateLocalDateCache.get(date).get().minusDays(1));
                    Double realizedPnLTMinus1 =
                        typeMapMap.get(accountType).floorEntry(dateTMinus1).getValue();

                    typeMapMap
                        .get(accountType)
                        .compute(
                            date,
                            (k1, v1) -> {
                              double dividendSum = 0.0;
                              for (DividendRecord record : records)
                                dividendSum += record.dividend();
                              return sanitizeDouble(v1)
                                  + dividendSum
                                  + sanitizeDouble(realizedPnLTMinus1);
                            });
                  }));
    }

    // compute for adding to realizedWithDividendDatePnLMap
    realizedWithDividendDatePnLMap.put(0, new HashMap<>());
    accountTypes.forEach(type -> realizedWithDividendDatePnLMap.get(0).put(type, 0.0)); // baseline
    dates.forEach(
        localDate -> {
          int date = dateLocalDateCache.get(localDate).getAsInt();
          realizedWithDividendDatePnLMap.putIfAbsent(date, new HashMap<>());
          Map<MarketDataProto.AccountType, Double> typePnLRealizedWithDivDatePnLMap =
              realizedWithDividendDatePnLMap.get(date);

          accountTypes.forEach(
              accountType -> {
                typePnLRealizedWithDivDatePnLMap.putIfAbsent(accountType, 0.0);

                //                System.out.println(date);
                //                System.out.println(accountType);

                int dateTMinus1 =
                    DateFormatUtil.getDate(dateLocalDateCache.get(date).get().minusDays(1));
                Double realizedTMinus1 =
                    realizedWithDividendDatePnLMap
                        .floorEntry(dateTMinus1)
                        .getValue()
                        .get(accountType);

                Double dividendValue =
                    dateDividendsMap.containsKey(date)
                        ? dateDividendsMap.get(date).get(accountType)
                        : null;
                Double realizedValue =
                    realizedDatePnLMap.containsKey(date)
                        ? realizedDatePnLMap.get(date).get(accountType)
                        : null;

                double totalValue =
                    sanitizeDouble(realizedTMinus1)
                        + sanitizeDouble(dividendValue)
                        + sanitizeDouble(realizedValue);
                typePnLRealizedWithDivDatePnLMap.compute(
                    accountType, (k, v) -> sanitizeDouble(v) + totalValue);
              });
        });
  }

  private Set<Integer> getDividendDates() {
    return dateDividendsMap.keySet();
  }

  private void computeRealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getInstrument().getQty();

    double tickerPrice = node.getPrev().getAcb().getAcbPerUnit();
    double pnL = (marketPrice - tickerPrice) * sellQty;

    if (date == TODAY_DATE)
      log.info(
          "realized pnL {} x {} x {} x {} => mkt price '{}', qty '{}', ticker price '{}' => pnl= {}",
          imnt,
          type.name(),
          date,
          node.getInstrument().getDirection().name(),
          marketPrice,
          sellQty,
          tickerPrice,
          pnL);
    realizedDatePnLMap.computeIfAbsent(
        date,
        k -> {
          Map<MarketDataProto.AccountType, Double> typePriceMap = new HashMap<>();
          getAccountTypes().forEach(accountType -> typePriceMap.put(accountType, 0.0));
          return typePriceMap;
        });
    realizedImntPnLMap.computeIfAbsent(
        imnt,
        k -> {
          Map<MarketDataProto.AccountType, Map<Integer, Double>> typeDatePriceMap = new HashMap<>();
          getAccountTypes()
              .forEach(accountType -> typeDatePriceMap.put(accountType, new HashMap<>()));
          return typeDatePriceMap;
        });

    realizedDatePnLMap.get(date).compute(type, (k, v) -> sanitizeDouble(v) + pnL);
    realizedImntPnLMap.get(imnt).get(type).compute(date, (k, v) -> sanitizeDouble(v) + pnL);
  }

  private void computeUnrealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getRunningQuantity();

    double tickerPrice = node.getAcb().getAcbPerUnit();
    double pnL = (marketPrice - tickerPrice) * sellQty;

    if (date == TODAY_DATE)
      log.info(
          "unrealized pnL {} x {} x {} x {} => mkt price '{}', qty '{}', ticker price '{}' => pnl= {}",
          imnt,
          type.name(),
          date,
          node.getInstrument().getDirection().name(),
          marketPrice,
          sellQty,
          tickerPrice,
          pnL);
    unrealizedDatePnLMap.computeIfAbsent(
        date,
        k -> {
          Map<MarketDataProto.AccountType, Double> typePriceMap = new HashMap<>();
          getAccountTypes().forEach(accountType -> typePriceMap.put(accountType, 0.0));
          return typePriceMap;
        });
    unrealizedImntPnLMap.computeIfAbsent(
        imnt,
        k -> {
          Map<MarketDataProto.AccountType, TreeMap<Integer, Double>> typeDatePriceMap =
              new HashMap<>();
          getAccountTypes()
              .forEach(accountType -> typeDatePriceMap.put(accountType, new TreeMap<>()));
          return typeDatePriceMap;
        });

    unrealizedDatePnLMap.get(date).compute(type, (k, v) -> sanitizeDouble(v) + pnL);
    unrealizedImntPnLMap.get(imnt).get(type).compute(date, (k, v) -> sanitizeDouble(v) + pnL);
  }

  private void computeCombinedPnL(List<LocalDate> dates) {
    List<MarketDataProto.AccountType> accountTypes = getAccountTypes();

    dates.forEach(
        localDate -> {
          int date = dateLocalDateCache.get(localDate).getAsInt();
          for (MarketDataProto.AccountType type : accountTypes) {
            Double unrealizedPnL = null;
            Double realizedPnL = null;
            if (unrealizedDatePnLMap.containsKey(date)) {
              unrealizedPnL = unrealizedDatePnLMap.get(date).get(type);
            }
            if (realizedDatePnLMap.containsKey(date)) {
              realizedPnL = realizedDatePnLMap.get(date).get(type);
            }
            if (realizedPnL == null && unrealizedPnL == null) {
              continue;
            }
            double combinedPnL = sanitizeDouble(unrealizedPnL) + sanitizeDouble(realizedPnL);

            if (date == TODAY_DATE)
              log.info("combined pnL {} x {} => pnl= {}", date, type.name(), combinedPnL);

            combinedDatePnLMap.computeIfAbsent(date, k -> new HashMap<>());
            combinedDatePnLMap.get(date).compute(type, (k, v) -> sanitizeDouble(v) + combinedPnL);
          }
        });
  }

  private void computeCombinedPnLCumulative() {
    combinedDatePnLCumulativeMap.putIfAbsent(0, new HashMap<>());
    getAccountTypes()
        .forEach(accountType -> combinedDatePnLCumulativeMap.get(0).put(accountType, 0.0));

    combinedDatePnLMap.forEach(
        (date, accountTypePnLMap) ->
            accountTypePnLMap.forEach(
                ((accountType, combinedPnL) -> {
                  Double realizedImntWithDivPnL =
                      realizedWithDividendDatePnLMap.floorEntry(date).getValue().get(accountType);
                  Double unrealizedPnL =
                      unrealizedDatePnLMap.containsKey(date)
                          ? unrealizedDatePnLMap.get(date).get(accountType)
                          : null;
                  double cumulativeCombinedPnL =
                      sanitizeDouble(realizedImntWithDivPnL) + sanitizeDouble(unrealizedPnL);
                  Map<MarketDataProto.AccountType, Double> typeDoubleMap =
                      combinedDatePnLCumulativeMap.computeIfAbsent(date, k -> new HashMap<>());
                  typeDoubleMap.put(accountType, cumulativeCombinedPnL);
                })));
  }

  private int getDate(DataNode node) {
    return node.getInstrument().getTicker().getData(0).getDate();
  }

  /*private int getNextMarketDate(int date) {
    LocalDate localDate = DateFormatUtil.getLocalDate(date);
    for (int i = 0; i <= 5; i++) {
      localDate = localDate.plusDays(1);
      if (dateAndLocalDateMap.containsKey(date)) return DateFormatUtil.getDate(localDate);
    }
    return -1; // what if div date is on a non trading date beyond last trading date of benchmark
    // ticker?
  }*/

  public Set<String> getInstruments() {
    return marketData.keySet();
  }

  private List<MarketDataProto.AccountType> getAccountTypes() {
    return Arrays.stream(MarketDataProto.AccountType.values())
        .filter(t -> t != MarketDataProto.AccountType.UNRECOGNIZED)
        .toList();
  }

  private record ImntValuationCurrentPnLAndActual(
      String imnt, Double currentValuationPnL, Double investmentActual, Double pnlPercentage) {}
}
