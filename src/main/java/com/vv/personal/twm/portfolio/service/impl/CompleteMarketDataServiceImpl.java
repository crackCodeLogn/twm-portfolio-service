package com.vv.personal.twm.portfolio.service.impl;

import static com.vv.personal.twm.portfolio.util.SanitizerUtil.sanitizeAndFormat2Double;
import static com.vv.personal.twm.portfolio.util.SanitizerUtil.sanitizeDouble;
import static java.lang.Double.NaN;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.cache.KeyInstrumentValueCache;
import com.vv.personal.twm.portfolio.model.market.DataList;
import com.vv.personal.twm.portfolio.model.market.DataNode;
import com.vv.personal.twm.portfolio.model.market.DividendRecord;
import com.vv.personal.twm.portfolio.model.tracking.ProgressTracker;
import com.vv.personal.twm.portfolio.remote.feign.CalcPythonEngine;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market.outdated.OutdatedSymbols;
import com.vv.personal.twm.portfolio.service.CompleteMarketDataService;
import com.vv.personal.twm.portfolio.service.ComputeMarketStatisticsService;
import com.vv.personal.twm.portfolio.service.ExtractMarketPortfolioDataService;
import com.vv.personal.twm.portfolio.service.InstrumentMetaDataService;
import com.vv.personal.twm.portfolio.service.ProgressTrackerService;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.DataConverterUtil;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.util.SanitizerUtil;
import com.vv.personal.twm.portfolio.util.math.StatisticsUtil;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
  private static final String CLIENT_VIVEK = "vivek-v2";
  private static final int TODAY_DATE = DateFormatUtil.getDate(LocalDate.now());

  private static final String UNKNOWN_SECTOR = "UNKNOWN";
  private static final String KEY_ACCOUNT_TYPE = "accountType";
  private static final String KEY_ACCOUNT_TYPES = "accountTypes";
  private static final String KEY_BOOK_VAL = "bookVal";
  private static final String KEY_CURRENT_VAL = "currentVal";
  private static final String KEY_DIV_YIELD_PERCENT = "divYieldPercent";
  private static final String KEY_IMNT = "imnt";
  private static final String KEY_PNL = "pnl";
  private static final String KEY_QTY = "qty";
  private static final String KEY_SECTOR = "sector";
  private static final String KEY_TOTAL_DIV = "totalDiv";
  private static final String KEY_TOTAL_IMNTS = "totalInstruments";
  private static final String KEY_WBETA = "beta-weighted";
  private static final String KEY_BETA = "beta";

  private static final Map<MarketDataProto.Country, String> COUNTRY_MARKET_RETURN_SYMBOL_MAP =
      ImmutableMap.<MarketDataProto.Country, String>builder()
          .put(MarketDataProto.Country.CA, "^GSPTSE") // S&P/TSX Composite Index
          .put(MarketDataProto.Country.US, "^GSPC") // S&P 500 Index
          .put(MarketDataProto.Country.IN, "^NSEI") // Nifty 50 Index
          .build();

  private static final Map<MarketDataProto.Country, String>
      COUNTRY_RISK_FREE_RETURN_CUSTOM_SYMBOL_MAP =
          ImmutableMap.<MarketDataProto.Country, String>builder() // 10 year treasury bonds
              .put(MarketDataProto.Country.CA, "^RF-CA") // BD.CDN.10YR.DQ.YLD
              .put(MarketDataProto.Country.US, "^RF-US") // 10 YR
              .build();

  private static final boolean IGNORE_ALL_EXCEPT_EQUITY_FOR_OPTIMIZER = true;

  // Holds map of ticker x (map of account type x doubly linked list nodes of transactions done)
  private final Map<String, Map<MarketDataProto.AccountType, DataList>> marketData;
  private final Map<
          String, Map<MarketDataProto.AccountType, TreeMap<Integer, List<DividendRecord>>>>
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

  private final Set<String> imntsNotInPortfolio;

  private final DateLocalDateCache dateLocalDateCache;
  private final KeyInstrumentValueCache keyInstrumentValueCache;
  private final InstrumentMetaDataService instrumentMetaDataService;
  private final ExtractMarketPortfolioDataService extractMarketPortfolioDataService;
  private final TickerDataWarehouseService tickerDataWarehouseService;
  private final MarketDataPythonEngineFeign marketDataPythonEngineFeign;
  private final ProgressTrackerService progressTrackerService;
  private final ComputeMarketStatisticsService computeMarketStatisticsService;
  private final MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;
  private final CalcPythonEngine calcPythonEngine;

  private Optional<Table<String, String, Double>> correlationMatrix;
  private OutdatedSymbols outdatedSymbols;
  private boolean isReloadInProgress;
  private List<LocalDate> localDates;
  private List<Integer> integerDates;

  public CompleteMarketDataServiceImpl(
      DateLocalDateCache dateLocalDateCache,
      KeyInstrumentValueCache keyInstrumentValueCache,
      InstrumentMetaDataService instrumentMetaDataService,
      ExtractMarketPortfolioDataService extractMarketPortfolioDataService,
      TickerDataWarehouseService tickerDataWarehouseService,
      MarketDataPythonEngineFeign marketDataPythonEngineFeign,
      CalcPythonEngine calcPythonEngine,
      ProgressTrackerService progressTrackerService,
      ComputeMarketStatisticsService computeMarketStatisticsService,
      MarketDataCrdbServiceFeign marketDataCrdbServiceFeign) {
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
    correlationMatrix = Optional.empty();
    localDates = new ArrayList<>();
    integerDates = new ArrayList<>();
    imntsNotInPortfolio = new HashSet<>();

    this.tickerDataWarehouseService = tickerDataWarehouseService;
    this.dateLocalDateCache = dateLocalDateCache;
    this.keyInstrumentValueCache = keyInstrumentValueCache;
    this.instrumentMetaDataService = instrumentMetaDataService;
    this.extractMarketPortfolioDataService = extractMarketPortfolioDataService;
    this.marketDataPythonEngineFeign = marketDataPythonEngineFeign;
    this.progressTrackerService = progressTrackerService;
    this.isReloadInProgress = false;

    this.computeMarketStatisticsService = computeMarketStatisticsService;
    this.marketDataCrdbServiceFeign = marketDataCrdbServiceFeign;
    this.calcPythonEngine = calcPythonEngine;
  }

  @Override
  public void load() {
    log.info("Initiating complete market data load");
    StopWatch stopWatch = StopWatch.createStarted();
    progressTrackerService.publishProgressTracker(CLIENT_VIVEK, ProgressTracker.LOADING_MARKET);

    // first populate the buy side
    progressTrackerService.publishProgressTracker(
        CLIENT_VIVEK, ProgressTracker.LOADING_MARKET_POPULATE_PORTFOLIO);
    populate(
        extractMarketPortfolioDataService
            .extractMarketPortfolioData(MarketDataProto.Direction.BUY)
            .getPortfolio());
    // then populate the sell side
    populate(
        extractMarketPortfolioDataService
            .extractMarketPortfolioData(MarketDataProto.Direction.SELL)
            .getPortfolio());

    progressTrackerService.publishProgressTracker(
        CLIENT_VIVEK, ProgressTracker.LOADING_MARKET_COMPUTE_ACB);
    computeAcb(); // compute the ACB once all the data has been populated

    progressTrackerService.publishProgressTracker(
        CLIENT_VIVEK, ProgressTracker.LOADING_MARKET_POPULATE_DIVIDENDS);
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
    progressTrackerService.publishProgressTracker(
        CLIENT_VIVEK, ProgressTracker.LOADING_MARKET_LOAD_ANALYSIS);
    tickerDataWarehouseService.loadAnalysisDataForInstruments(getInstruments(), isReloadInProgress);

    this.imntsNotInPortfolio.addAll(
        tickerDataWarehouseService.loadAnalysisDataForInstrumentsNotInPortfolio(
            getInstruments(), isReloadInProgress, this.imntsNotInPortfolio));

    progressTrackerService.publishProgressTracker(
        CLIENT_VIVEK, ProgressTracker.LOADING_MARKET_COMPUTE_PNL);
    computePnL();

    computeCorrelationMatrixInParallel();

    progressTrackerService.publishProgressTracker(
        CLIENT_VIVEK, ProgressTracker.LOADING_MARKET_COMPUTE_CUMULATIVE_DIVIDENDS);
    computeCumulativeDividend();

    progressTrackerService.publishProgressTracker(
        CLIENT_VIVEK, ProgressTracker.LOADING_MARKET_COMPUTE_SECTOR_AGGR);
    computeSectorLevelImntAggregationData();

    progressTrackerService.publishProgressTracker(CLIENT_VIVEK, ProgressTracker.READY_MARKET);
    stopWatch.stop();
    log.info(
        "Complete market data load finished in {}ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  private void computeCorrelationMatrixInParallel() {
    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    try {
      Set<String> combinedImnts = new HashSet<>(imntsNotInPortfolio);
      combinedImnts.addAll(getInstruments());
      // todo - later note: all imnt not in portfolio are ending up getting excluded from corr
      // compute, work on it later if time be.

      Future<List<String>> imntsForCorrelationFuture =
          singleThreadExecutor.submit(
              () ->
                  getInstrumentSelectionForCorrelationMatrixCompute(
                      localDates, tickerDataWarehouseService, combinedImnts));
      singleThreadExecutor.shutdown();

      List<String> imntsForCorrelation = imntsForCorrelationFuture.get();
      log.info(
          "Beginning compute of correlation matrix for {} instruments", imntsForCorrelation.size());
      combinedImnts.clear();

      // correlation matrix compute happens here
      this.correlationMatrix =
          computeMarketStatisticsService.computeCorrelationMatrix(
              imntsForCorrelation, integerDates);
      log.debug("Correlation matrix => {}", this.correlationMatrix);
    } catch (ExecutionException | InterruptedException e) {
      log.error("Compute correlation matrix execution failed", e);
    } finally {
      singleThreadExecutor.shutdownNow();
    }
  }

  List<String> getInstrumentSelectionForCorrelationMatrixCompute(
      List<LocalDate> benchmarkLocalDates,
      TickerDataWarehouseService tickerDataWarehouseService,
      Set<String> imnts) {
    List<String> imntsForCorrelation = new ArrayList<>(100);
    for (String imnt : imnts) {
      log.debug("Checking eligibility of imnt {} for correlation compute", imnt);
      boolean selectImntForCorrelationCompute = true;
      for (LocalDate date : benchmarkLocalDates) {
        if (!tickerDataWarehouseService.containsMarketData(imnt, date)) {
          selectImntForCorrelationCompute = false;
          break;
        }
      }
      if (selectImntForCorrelationCompute) imntsForCorrelation.add(imnt);
      else log.warn("Ignoring imnt {} for correlation matrix compute", imnt);
    }
    return imntsForCorrelation;
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
    correlationMatrix = Optional.empty();
    integerDates.clear();
    localDates.clear();
    dateLocalDateCache.flush();
    keyInstrumentValueCache.flushAll();
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
    Map<String, List<Double>> imntInfoListMap = new HashMap<>();

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
              double qty = lastImntNode.getRunningQuantity();

              imntInfoListMap.put(
                  imnt, Lists.newArrayList(currentValuationPnL, investmentActual, qty));
            }
          }
        });

    if (includeDividends) {
      log.info("Including dividend and sells in the best and worst performers calculation");
      imntInfoListMap.forEach(
          (imnt, infoList) -> {
            if (realizedImntWithDividendPnLMap.containsKey(imnt)
                && realizedImntWithDividendPnLMap.get(imnt).containsKey(accountType)) {
              double combinedValuation =
                  infoList.get(0)
                      + realizedImntWithDividendPnLMap
                          .get(imnt)
                          .get(accountType)
                          .floorEntry(TODAY_DATE)
                          .getValue();
              infoList.set(0, combinedValuation);
            }
          });
    }

    List<ImntValuationCurrentPnLAndActual> collectionData = new ArrayList<>(imntInfoListMap.size());
    for (Map.Entry<String, List<Double>> entry : imntInfoListMap.entrySet()) {
      Double pnlPercentage = (entry.getValue().get(0) * 100.0 / entry.getValue().get(1));
      if (pnlPercentage != NaN) {
        collectionData.add(
            new ImntValuationCurrentPnLAndActual(
                entry.getKey(),
                entry.getValue().get(0), // current valuation
                entry.getValue().get(1), // actual investment
                pnlPercentage,
                entry.getValue().get(2))); // qty
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
          log.debug(
              "[{}] {} => {}, {}, {}, {}",
              accountType,
              imntValuationCurrentPnLAndActual.imnt(),
              imntValuationCurrentPnLAndActual.currentValuationPnL(),
              imntValuationCurrentPnLAndActual.investmentActual(),
              imntValuationCurrentPnLAndActual.pnlPercentage(),
              imntValuationCurrentPnLAndActual.qty());

          bestAndWorstPerformerMap.put(
              imntValuationCurrentPnLAndActual.imnt(),
              String.format(
                  "%.02f|%.02f|%.02f",
                  imntValuationCurrentPnLAndActual.currentValuationPnL(),
                  imntValuationCurrentPnLAndActual.investmentActual(),
                  imntValuationCurrentPnLAndActual.qty()));
        });

    return bestAndWorstPerformerMap;
  }

  @Override
  public List<String> getMarketValuations(boolean includeDividendsForCurrentVal) {
    List<String> valuations = new ArrayList<>(marketData.size());
    Map<String, Map<String, String>> imntValuationMap =
        getImntValuationPortfolioLevel(includeDividendsForCurrentVal);
    imntValuationMap
        .values()
        .forEach(
            kv -> {
              List<String> vals = new ArrayList<>(kv.size());
              kv.forEach((k, v) -> vals.add(String.format("%s=%s", k, v)));
              valuations.add(StringUtils.join(vals, "|"));
            });
    return valuations;
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
            ? cumulativeImntDividendsMap.get(imnt).getOrDefault(accountType, 0.0)
            : 0.0;
    double divYieldPercent = instrumentMetaDataService.getDividendYield(imnt).orElse(0.0);

    marketValuation.put(KEY_IMNT, imnt);
    marketValuation.put(KEY_ACCOUNT_TYPE, accountType.name());
    marketValuation.put(KEY_SECTOR, node.getInstrument().getTicker().getSector());
    marketValuation.put(KEY_DIV_YIELD_PERCENT, sanitizeAndFormat2Double(divYieldPercent));
    marketValuation.put(KEY_BOOK_VAL, sanitizeAndFormat2Double(bookVal));
    marketValuation.put(KEY_CURRENT_VAL, sanitizeAndFormat2Double(currentVal));
    marketValuation.put(KEY_PNL, sanitizeAndFormat2Double(pnl));
    marketValuation.put(KEY_TOTAL_DIV, sanitizeAndFormat2Double(totalDiv));

    log.info("Computed market valuation for {} x {}", imnt, accountType);
    return marketValuation;
  }

  @Override
  public Map<String, String> getAllImntsSector() {
    return imntSectorMap;
  }

  @Override
  public void shutdown() {
    log.info("Complete Market Data Service shutdown");
  }

  @Override
  public Map<String, Double> getNetMarketValuations(
      Optional<MarketDataProto.AccountType> optionalAccountType,
      boolean includeDividendsForCurrentVal) {
    Map<String, Integer> keyLookup =
        Map.of(KEY_BOOK_VAL, 0, KEY_PNL, 1, KEY_TOTAL_DIV, 2, KEY_QTY, 3);
    List<Double> values = Lists.newArrayList(0.0, 0.0, 0.0, 0.0); // to store above 4 vals in-place
    Set<String> imnts = new HashSet<>();
    Map<String, Pair<Double, Double>> imntBetaCurrentValue = new HashMap<>();

    marketData.forEach(
        (imnt, accountTypeDataListMap) -> {
          Set<MarketDataProto.AccountType> accountTypesToLookAt =
              optionalAccountType.isEmpty()
                  ? accountTypeDataListMap.keySet()
                  : Sets.newHashSet(optionalAccountType.get());

          List<Double> currentVal = Lists.newArrayList(0.0);
          accountTypesToLookAt.stream()
              .filter(accountTypeDataListMap::containsKey)
              .forEach(
                  accountType -> {
                    imnts.add(imnt);
                    DataNode node = accountTypeDataListMap.get(accountType).getTail();
                    double bookVal = node.getAcb().getAcbPerUnit() * node.getRunningQuantity();
                    double pnl =
                        unrealizedImntPnLMap
                            .get(imnt)
                            .get(accountType)
                            .floorEntry(TODAY_DATE)
                            .getValue();
                    double totalDiv =
                        cumulativeImntDividendsMap.containsKey(imnt)
                            ? cumulativeImntDividendsMap.get(imnt).getOrDefault(accountType, 0.0)
                            : 0.0;
                    double qty = node.getRunningQuantity();
                    double pnlWithDiv = pnl + (includeDividendsForCurrentVal ? totalDiv : 0.0);

                    values.set(
                        keyLookup.get(KEY_BOOK_VAL),
                        values.get(keyLookup.get(KEY_BOOK_VAL)) + bookVal);
                    values.set(
                        keyLookup.get(KEY_PNL), values.get(keyLookup.get(KEY_PNL)) + pnlWithDiv);
                    values.set(
                        keyLookup.get(KEY_TOTAL_DIV),
                        values.get(keyLookup.get(KEY_TOTAL_DIV)) + totalDiv);
                    values.set(keyLookup.get(KEY_QTY), values.get(keyLookup.get(KEY_QTY)) + qty);

                    currentVal.set(0, currentVal.get(0) + bookVal + pnlWithDiv);
                  });

          Optional<Double> optionalBeta = instrumentMetaDataService.getBeta(imnt);
          if (optionalBeta.isEmpty()) {
            log.warn("Not considering {} for beta calc as beta was not found", imnt);
          } else {
            imntBetaCurrentValue.put(imnt, Pair.of(optionalBeta.get(), currentVal.get(0)));
          }
        });
    double currentValForBeta = 0.0;
    for (Pair<Double, Double> pair : imntBetaCurrentValue.values())
      currentValForBeta += pair.getRight();

    double currentVal =
        values.get(keyLookup.get(KEY_BOOK_VAL)) + values.get(keyLookup.get(KEY_PNL));

    log.info(
        "Delta {} > {} from cv: {}, tccv: {}",
        optionalAccountType,
        currentVal - currentValForBeta,
        currentVal,
        currentValForBeta);
    Double weightedBeta =
        currentValForBeta == 0.0
            ? NaN
            : getWeightedBeta(imntBetaCurrentValue.values(), currentValForBeta);

    return ImmutableMap.<String, Double>builder()
        .put(KEY_TOTAL_IMNTS, (double) imnts.size())
        .put(KEY_BOOK_VAL, values.get(keyLookup.get(KEY_BOOK_VAL)))
        .put(KEY_PNL, values.get(keyLookup.get(KEY_PNL)))
        .put(KEY_CURRENT_VAL, currentVal)
        .put(KEY_TOTAL_DIV, values.get(keyLookup.get(KEY_TOTAL_DIV)))
        .put(KEY_QTY, values.get(keyLookup.get(KEY_QTY)))
        .put(KEY_WBETA, weightedBeta)
        .build();
  }

  @Override
  public int forceDownloadMarketDataForDates(String imnt, String startDate, String endDate) {
    MarketDataProto.Ticker tickerData =
        marketDataPythonEngineFeign.getTickerDataWithoutCountryCode(imnt, startDate, endDate);
    if (tickerData == null) {
      log.error("Failed to obtain data for {} from {} to {}", imnt, startDate, endDate);
      return 0;
    }

    List<Integer> tickerDates =
        tickerData.getDataList().stream().map(MarketDataProto.Value::getDate).toList();
    log.info("Retrieved {} records from market data API for {}", tickerDates.size(), imnt);

    log.info(
        "Attempting purge of {} records from market data database for {}",
        tickerDates.size(),
        imnt);

    try {
      marketDataCrdbServiceFeign.deleteMarketData(imnt, tickerDates);
    } catch (Exception e) {
      log.error("Failed to delete market data for {} x {} dates", imnt, tickerDates.size(), e);
    }

    log.info("Publishing {} records of {} to ticker data warehouse", tickerDates.size(), imnt);
    tickerDataWarehouseService.fillAnalysisWarehouse(tickerData);

    log.info("Publishing {} records of {} to market data database", tickerDates.size(), imnt);
    String result = marketDataCrdbServiceFeign.addMarketDataForSingleTicker(tickerData);

    log.info("Market data save result: {}", result);
    return tickerDates.size();
  }

  @Override
  public Optional<Table<String, String, Double>> getCorrelationMatrix(
      List<String> targetInstruments) {
    Optional<Table<String, String, Double>> optionalMatrix = correlationMatrix;
    if (optionalMatrix.isPresent()) {
      Table<String, String, Double> matrix = optionalMatrix.get();
      Queue<String> newImnts = new LinkedList<>();
      Queue<String> knownImnts = new LinkedList<>();

      for (String imnt : targetInstruments) {
        imnt = imnt.toUpperCase();
        if (!matrix.rowKeySet().contains(imnt)) { // imnt we don't have data for!
          if (localDates.isEmpty()) {
            throw new RuntimeException(
                "Unstable state where localDates is empty. Shouldn't happen ever!");
          }
          MarketDataProto.Ticker dbTickerData =
              marketDataCrdbServiceFeign.getMarketDataByTicker(imnt);
          if (dbTickerData != null && dbTickerData.getDataCount() > 0) {
            log.info(
                "Found market data for {} from db => {} records",
                imnt,
                dbTickerData.getDataCount());
            tickerDataWarehouseService.fillAnalysisWarehouse(dbTickerData);
          } else {
            log.info("Querying market data API for {}", imnt);
            int recordsDownloaded =
                forceDownloadMarketDataForDates(
                    imnt,
                    localDates.get(0).toString(),
                    localDates.get(localDates.size() - 1).toString());
            if (recordsDownloaded == 0) {
              log.warn("Cannot download missing market data for unknown imnt {}", imnt);
              continue;
            }
          }
          newImnts.offer(imnt);
        } else knownImnts.offer(imnt);
      }

      if (newImnts.isEmpty() && knownImnts.isEmpty()) {
        log.warn(
            "Weird state when the correlation matrix compute for {} target imnts led to no known / new imnts",
            targetInstruments.size());
      } else if (newImnts.isEmpty()) { // no need to re-compute the correlation matrix
        List<String> knownInstruments = new ArrayList<>();
        while (!knownImnts.isEmpty()) knownInstruments.add(knownImnts.poll());
        Table<String, String, Double> resultantMatrix = HashBasedTable.create();

        for (int i = 0; i < knownInstruments.size(); i++)
          for (int j = i + 1; j < knownInstruments.size(); j++) {
            String imnt1 = knownInstruments.get(i);
            String imnt2 = knownInstruments.get(j);
            Double val = matrix.get(imnt1, imnt2);
            resultantMatrix.put(imnt1, imnt2, val);
            resultantMatrix.put(imnt2, imnt1, val);
          }

        return Optional.of(resultantMatrix);

      } else { // generate new correlation matrix
        List<String> instruments = new ArrayList<>();
        while (!newImnts.isEmpty()) instruments.add(newImnts.poll());
        while (!knownImnts.isEmpty()) instruments.add(knownImnts.poll());

        return computeMarketStatisticsService.computeCorrelationMatrix(instruments, integerDates);
      }
    }
    return Optional.empty();
  }

  // NOTE: works only for imnts whose information is present
  @Override
  public OptionalDouble getCorrelation(String imnt1, String imnt2) {
    Optional<Double> correlation =
        computeMarketStatisticsService.computeCorrelation(imnt1, imnt2, integerDates);
    return correlation.map(OptionalDouble::of).orElseGet(OptionalDouble::empty);
  }

  @Override
  public Optional<Table<String, String, Double>> getCorrelationMatrix(
      MarketDataProto.AccountType accType) {
    Optional<Table<String, String, Double>> optionalMatrix = correlationMatrix;
    if (optionalMatrix.isPresent()) {
      List<String> imnts = new ArrayList<>();
      marketData.forEach(
          (imnt, accountTypeValueMap) -> {
            if (accountTypeValueMap.containsKey(accType)
                && optionalMatrix.get().rowKeySet().contains(imnt)
                && accountTypeValueMap.get(accType).getTail().getRunningQuantity() >= 0.001)
              imnts.add(imnt);
          });
      return getCorrelationMatrix(imnts);
    }
    return Optional.empty();
  }

  @Override
  public Optional<Table<String, String, Double>> getCorrelationMatrixForSectors() {
    Map<String, Map<String, String>> imntValuationPortfolioLevel =
        getImntValuationPortfolioLevel(true);
    Map<String, Pair<String, Double>> sectorMaxImntValuationMap = new HashMap<>();
    Map<String, String> imntNewLabelMap = new HashMap<>();

    imntSectorMap.forEach(
        (imnt, sector) -> {
          Double imntCurrentVal =
              Double.parseDouble(imntValuationPortfolioLevel.get(imnt).get(KEY_CURRENT_VAL));
          imntNewLabelMap.put(
              imnt,
              String.format(
                  "%s|%s",
                  SanitizerUtil.sanitizeStringOnLength(
                      imntValuationPortfolioLevel.get(imnt).get(KEY_SECTOR), 7),
                  imnt.substring(0, imnt.lastIndexOf('.'))));

          if (!sectorMaxImntValuationMap.containsKey(sector))
            sectorMaxImntValuationMap.put(sector, Pair.of(imnt, imntCurrentVal));
          else {
            Double existingSectorImntCurrentVal = sectorMaxImntValuationMap.get(sector).getValue();
            if (imntCurrentVal > existingSectorImntCurrentVal) {
              sectorMaxImntValuationMap.put(sector, Pair.of(imnt, imntCurrentVal));
            }
          }
        });

    log.info("Sector level max imnt details: {}", sectorMaxImntValuationMap);

    List<String> imnts = sectorMaxImntValuationMap.values().stream().map(Pair::getKey).toList();
    Optional<Table<String, String, Double>> correlationMatrix = getCorrelationMatrix(imnts);
    if (correlationMatrix.isEmpty()) {
      log.error("Failed to compute sector level correlation matrix");
      return Optional.empty();
    }
    Table<String, String, Double> sectorCorrelationMatrixResult = HashBasedTable.create();
    for (String rowImnt : correlationMatrix.get().rowKeySet()) {
      for (String colImnt : correlationMatrix.get().columnKeySet()) {
        Double correlation = correlationMatrix.get().get(rowImnt, colImnt);
        if (correlation != null)
          sectorCorrelationMatrixResult.put(
              imntNewLabelMap.get(rowImnt), imntNewLabelMap.get(colImnt), correlation);
      }
    }
    correlationMatrix = null; // force cleanup
    log.debug("Sector Correlation matrix => {}", sectorCorrelationMatrixResult);
    return Optional.of(sectorCorrelationMatrixResult);
  }

  @Override
  public int getBenchMarkCurrentDate() {
    // works on the assumption that this method will be called after computePnl() where the dates
    // have already been calculated via the ticker warehouse and sorted
    return integerDates.isEmpty() ? -1 : integerDates.get(integerDates.size() - 1);
  }

  @Override
  public DataPacketProto.DataPacket getHeadingAtAGlance() {
    // ^VIX, 1/CADUSD=X, CADINR=X
    DataPacketProto.DataPacket.Builder dataPacketBuilder = DataPacketProto.DataPacket.newBuilder();

    String imnt;
    Optional<Double> data;

    imnt = "^VIX";
    data = fetchLatestPrice(imnt, TODAY_DATE);
    if (data.isPresent()) dataPacketBuilder.putStringDoubleMap(imnt, data.get());

    imnt = "CADINR=X";
    data = fetchLatestPrice(imnt, TODAY_DATE);
    if (data.isPresent()) dataPacketBuilder.putStringDoubleMap(imnt, data.get());

    imnt = "CADUSD=X";
    data = fetchLatestPrice(imnt, TODAY_DATE);
    if (data.isPresent() && data.get() >= 0.000001)
      dataPacketBuilder.putStringDoubleMap("USDCAD=X", 1.0 / data.get());

    imnt = "USDINR=X";
    data = fetchLatestPrice(imnt, TODAY_DATE);
    if (data.isPresent()) dataPacketBuilder.putStringDoubleMap(imnt, data.get());

    return dataPacketBuilder.build();
  }

  @Override
  public Optional<Double> getLatestRiskFreeReturn(MarketDataProto.Country country) {
    if (!COUNTRY_RISK_FREE_RETURN_CUSTOM_SYMBOL_MAP.containsKey(country)) {
      log.warn("Did not recognize country {} for latest risk free return extraction", country);
      return Optional.empty();
    }
    // TODO - update bank of canada data extraction logic later via csv/json:
    // https://www.bankofcanada.ca/valet/observations/group/bond_yields_benchmark/csv
    // main website: https://www.bankofcanada.ca/rates/interest-rates/canadian-bonds

    if (country == MarketDataProto.Country.CA) return Optional.of(3.35); // as of 20260116
    if (country == MarketDataProto.Country.US) return Optional.of(4.24); // as of 20260116
    return Optional.empty();
  }

  @Override
  public Optional<Double> getLatestMarketReturn(MarketDataProto.Country country) {
    if (!COUNTRY_MARKET_RETURN_SYMBOL_MAP.containsKey(country)) {
      log.warn("Did not recognize country {} for latest market return extraction", country);
      return Optional.empty();
    }

    Optional<Double> currentValue =
        fetchLatestPrice(COUNTRY_MARKET_RETURN_SYMBOL_MAP.get(country), TODAY_DATE);
    if (currentValue.isPresent()) {
      LocalDate tMinus1Year = DateFormatUtil.getLocalDate(TODAY_DATE).minusYears(1);
      Optional<Double> tMinus1YearValue =
          fetchLatestPrice(
              COUNTRY_MARKET_RETURN_SYMBOL_MAP.get(country), DateFormatUtil.getDate(tMinus1Year));
      return StatisticsUtil.calculateChangePercentage(tMinus1YearValue, currentValue);
    }

    return Optional.empty();
  }

  @Override
  public void testInfo() {
    List<String> imnts = new ArrayList<>(getInstruments());
    Collections.sort(imnts);

    // BETA first
    /*for (String imnt : imnts) {
      Optional<Double> betaFromMetaData = instrumentMetaDataService.getBeta(imnt);
      Optional<Double> betaCalculated =
          computeMarketStatisticsService.computeBeta(
              imnt, COUNTRY_MARKET_RETURN_SYMBOL_MAP.get(MarketDataProto.Country.CA), integerDates);
      if (betaFromMetaData.isPresent())
        log.info("Beta> M: [{}], C: [{}]", betaFromMetaData.get(), betaCalculated.get());
      else log.info("Beta> M: [{}], C: [{}]", betaFromMetaData, betaCalculated);
    }*/
    for (String imnt : imnts) {
      Optional<Double> pe = instrumentMetaDataService.getPE(imnt);
      log.info("PE (fwd): {} x {}", imnt, pe);
    }

    Optional<Double> rfr = getLatestRiskFreeReturn(MarketDataProto.Country.CA);
    log.info("rfr: {}", rfr);
    Optional<Double> marketReturn = getLatestMarketReturn(MarketDataProto.Country.CA);
    log.info("marketReturn: {}", marketReturn);

    for (String imnt : imnts) {
      MarketDataProto.InstrumentType imntType = instrumentMetaDataService.getInstrumentType(imnt);
      log.info("Imnt type: {} x {}", imnt, imntType);
    }

    for (String imnt : imnts) {
      Optional<Double> dividendYield = instrumentMetaDataService.getDividendYield(imnt);
      log.info("DivYield: {} x {}", imnt, dividendYield);
    }

    for (String imnt : imnts) {
      Optional<Double> beta = instrumentMetaDataService.getBeta(imnt);
      if (beta.isPresent()) {
        Double imntReturn =
            computeMarketStatisticsService.computeExpectedReturn(
                beta.get(), rfr.get(), marketReturn.get());
        log.info("Expected Return: {} x {}", imnt, imntReturn);
      } else log.warn("No expected return for {} because of no beta", imnt);
    }

    for (String imnt : imnts) {
      Optional<Double> std =
          computeMarketStatisticsService.computeStandardDeviationInFormOfEWMAVol(
              imnt, integerDates);
      log.info("STD: {} x {}", imnt, std);
    }
  }

  @Override
  public String invokePortfolioOptimizer(
      MarketDataProto.AccountType accountType,
      double targetBeta,
      double maxVol,
      double maxPe,
      double maxWeight,
      double minYield,
      double newCash,
      String objectiveMode) {
    log.info("Preparing data for invoking portfolio optimizer for {}", accountType);

    List<String> imntsInScope =
        marketData.entrySet().stream()
            .filter(e -> e.getValue().containsKey(accountType))
            .map(Map.Entry::getKey)
            .sorted()
            .toList();

    log.info(
        "Selected {} imnts for compute for portfolio optimizer for {}",
        imntsInScope.size(),
        accountType);

    // extracting single level values out for 1st imnt - manual
    Optional<Double> riskFreeReturn = getLatestRiskFreeReturn(MarketDataProto.Country.CA);
    if (riskFreeReturn.isEmpty()) {
      log.error(
          "Could not find latest risk free return for Canada. Cannot invoke portfolio optimizer");
      return "No latest RFR";
    }

    Optional<Double> marketReturn = getLatestMarketReturn(MarketDataProto.Country.CA);
    if (marketReturn.isEmpty()) {
      log.error("Could not find market return for Canada. Cannot invoke portfolio optimizer");
      return "No market return found";
    }
    // add to first imnt manual here

    // extracting imnt in scope level info
    List<MarketDataProto.Instrument> instruments = new ArrayList<>(imntsInScope.size());
    List<String> instrumentsForCorrelation = new ArrayList<>(imntsInScope.size());
    for (String imnt : imntsInScope) {
      MarketDataProto.InstrumentType imntType = instrumentMetaDataService.getInstrumentType(imnt);
      if (IGNORE_ALL_EXCEPT_EQUITY_FOR_OPTIMIZER
          && !imntType.equals(MarketDataProto.InstrumentType.EQUITY)) {
        log.warn("Skipping imnt {} for optimizer calc as it has type {}", imnt, imntType);
        continue;
      }

      Optional<Double> pe = instrumentMetaDataService.getPE(imnt);
      Optional<Double> betaFromMetaData = instrumentMetaDataService.getBeta(imnt);
      Optional<Double> dividendYield = instrumentMetaDataService.getDividendYield(imnt);
      double imntReturn =
          betaFromMetaData.isPresent()
              ? computeMarketStatisticsService.computeExpectedReturn(
                  betaFromMetaData.get(), riskFreeReturn.get(), marketReturn.get())
              : NaN;
      Optional<Double> std =
          computeMarketStatisticsService.computeStandardDeviationInFormOfEWMAVol(
              imnt, integerDates);

      if (betaFromMetaData.isEmpty()
          || dividendYield.isEmpty()
          || std.isEmpty()
          || pe.isEmpty()
          || Double.isNaN(imntReturn)) {
        log.warn(
            "Cannot select imnt {} due to one of the missing param: beta {}, div yield {}, std {}, pe {}, return {}",
            imnt,
            betaFromMetaData,
            dividendYield,
            std,
            pe,
            imntReturn);
        continue;
      }

      // use getMarketValuation(imnt, acctType) for getting the current val
      // although, later, rethink about the current val details to be used
      Map<String, String> marketValuation = getMarketValuation(imnt, accountType);
      double currentValue = Double.parseDouble(marketValuation.get(KEY_CURRENT_VAL));

      MarketDataProto.Instrument instrument =
          generateInstrument(
              imnt,
              currentValue,
              betaFromMetaData.get(),
              dividendYield.get() / 100.0,
              imntReturn / 100.0,
              std.get(),
              pe.get());
      instruments.add(instrument);
      instrumentsForCorrelation.add(imnt);
    }

    log.info("Generated imnt list with data of size {}", instruments.size());
    if (instruments.size() < 2) {
      log.error(
          "Selected imnts reduced to {} post imnt level extraction for optimizer. Cannot proceed",
          instruments.size());
      return "Too less selected imnts";
    }
    /*if (imntsInScope.size() - instruments.size() > 3) {
      log.warn(
          "{} instruments were removed in selection. Investigate why is that the case",
          imntsInScope.size() - instruments.size());
      return "Investigate imnts selection reduction";
    }*/

    Optional<Double> vixOpt = fetchLatestPrice("^VIX", TODAY_DATE);
    String riskMode = "CONSERVATIVE (HARVESTING PnL)";
    /*double targetBeta = 1.055;
    double maxVol = .35;
    double maxPe = 90.0;
    double maxWeight = .20;
    double minYield = .015;*/
    double vix = 16;

    if (vixOpt.isPresent()) {
      vix = vixOpt.get();
      if (vix > 25) {
        riskMode = "OPPORTUNISTIC (BUYING THE DIP)";
        /*maxVol = .18; // todo - removing for now, will restore once experiments are done
        maxPe = 18.0;
        targetBeta = 1.15;*/
      }
    }
    MarketDataProto.Instrument manualImntForOptimizer =
        generateManualInstrumentForOptimizer(
            vix, riskMode, targetBeta, maxVol, maxPe, maxWeight, minYield, newCash, objectiveMode);

    Optional<Table<String, String, Double>> correlationMatrixOpt =
        computeMarketStatisticsService.computeCorrelationMatrix(
            instrumentsForCorrelation, integerDates);
    MarketDataProto.CorrelationMatrix correlationMatrix =
        DataConverterUtil.getCorrelationMatrix(correlationMatrixOpt);
    if (correlationMatrix.getEntriesCount() == 0) {
      log.error("Correlation matrix is empty");
      return "Correlation matrix is empty";
    }

    MarketDataProto.Portfolio optimizerPortfolio =
        MarketDataProto.Portfolio.newBuilder()
            .setCorrelationMatrix(correlationMatrix)
            .addInstruments(manualImntForOptimizer)
            .addAllInstruments(instruments)
            .build();

    try {
      return calcPythonEngine.calcPortfolioOptimizer(optimizerPortfolio);
    } catch (Exception e) {
      log.error("Failed to process portfolio optimizer", e);
    }
    return "Failed to process portfolio optimizer";
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
      Map<MarketDataProto.AccountType, TreeMap<Integer, List<DividendRecord>>> divDateValueMap =
          imntDividendsMap.get(instrument.getTicker().getSymbol());
      divDateValueMap.computeIfAbsent(accountType, k -> new TreeMap<>());
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
    Collections.sort(dates);
    this.localDates = new ArrayList<>(dates);

    Set<Integer> localDates = new HashSet<>();
    dates.forEach(
        date -> {
          dateLocalDateCache.add(date);
          int intDate = dateLocalDateCache.get(date).getAsInt();
          localDates.add(intDate);
          this.integerDates.add(intDate);
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

        log.debug("Found dateIndex: {} for {} of {} {}", dateIndex, nodeDate, imnt, type);
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
      Map<MarketDataProto.AccountType, TreeMap<Integer, List<DividendRecord>>> accountTypeMapMap =
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

    // if (date == TODAY_DATE)
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

  private Map<String, Map<String, String>> getImntValuationPortfolioLevel(
      boolean includeDividendsForCurrentVal) {
    Map<String, Map<String, String>> imntValuationMap = new TreeMap<>();
    marketData.forEach(
        (imnt, accountTypeDataListMap) -> {
          Map<String, String> valuationDataMap = new HashMap<>();

          List<Double> values = Lists.newArrayList(0.0, 0.0, 0.0, 0.0);
          // 0 - book val, 1 - pnl, 2 - total div, 3 - qty
          List<String> accountTypes = new ArrayList<>();
          accountTypeDataListMap.forEach(
              (accountType, dataList) -> {
                accountTypes.add(accountType.name());

                DataNode node = dataList.getTail();
                double bookVal = node.getAcb().getAcbPerUnit() * node.getRunningQuantity();
                double pnl =
                    unrealizedImntPnLMap
                        .get(imnt)
                        .get(accountType)
                        .floorEntry(TODAY_DATE)
                        .getValue();
                double totalDiv =
                    cumulativeImntDividendsMap.containsKey(imnt)
                        ? cumulativeImntDividendsMap.get(imnt).getOrDefault(accountType, 0.0)
                        : 0.0;
                double qty = node.getRunningQuantity(); // from the tail

                values.set(0, values.get(0) + bookVal);
                values.set(
                    1, values.get(1) + pnl + (includeDividendsForCurrentVal ? totalDiv : 0.0));
                values.set(2, values.get(2) + totalDiv);
                values.set(3, values.get(3) + qty);
              });
          double currentVal = values.get(0) + values.get(1);
          Collections.sort(accountTypes);
          String accountTypesInUse = StringUtils.join(accountTypes, ", ");

          valuationDataMap.put(KEY_IMNT, imnt);
          valuationDataMap.put(KEY_ACCOUNT_TYPES, accountTypesInUse);
          valuationDataMap.put(KEY_SECTOR, imntSectorMap.getOrDefault(imnt, UNKNOWN_SECTOR));
          valuationDataMap.put(
              KEY_DIV_YIELD_PERCENT,
              sanitizeAndFormat2Double(instrumentMetaDataService.getDividendYield(imnt)));
          valuationDataMap.put(KEY_BOOK_VAL, sanitizeAndFormat2Double(values.get(0)));
          valuationDataMap.put(KEY_PNL, sanitizeAndFormat2Double(values.get(1)));
          valuationDataMap.put(KEY_TOTAL_DIV, sanitizeAndFormat2Double(values.get(2)));
          valuationDataMap.put(KEY_CURRENT_VAL, sanitizeAndFormat2Double(currentVal));
          valuationDataMap.put(KEY_QTY, sanitizeAndFormat2Double(values.get(3)));

          imntValuationMap.put(imnt, valuationDataMap);
        });
    return imntValuationMap;
  }

  private int getDate(DataNode node) {
    return node.getInstrument().getTicker().getData(0).getDate();
  }

  private Double getWeightedBeta(
      Collection<Pair<Double, Double>> betaCurrentVals, double totalCurrentVal) {
    double weightedBeta = 0.0;
    for (Pair<Double, Double> pair : betaCurrentVals)
      weightedBeta += pair.getKey() * pair.getValue();
    return weightedBeta / totalCurrentVal;
  }

  private MarketDataProto.Instrument generateManualInstrumentForOptimizer(
      Double vix,
      String riskMode,
      Double targetBeta,
      Double maxVol,
      Double maxPe,
      Double maxWeight,
      Double minYield,
      Double newCash,
      String objectiveMode) {
    return MarketDataProto.Instrument.newBuilder()
        .putMetaData("vix", String.valueOf(vix))
        .putMetaData("risk_mode", riskMode)
        .putMetaData("target_beta", String.valueOf(targetBeta))
        .putMetaData("max_vol", String.valueOf(maxVol))
        .putMetaData("max_pe", String.valueOf(maxPe))
        .putMetaData("max_weight", String.valueOf(maxWeight))
        .putMetaData("min_yield", String.valueOf(minYield))
        .putMetaData("new_cash", String.valueOf(newCash))
        .putMetaData("objective_mode", String.valueOf(objectiveMode))
        .build();
  }

  private MarketDataProto.Instrument generateInstrument(
      String symbol,
      double capital,
      double beta,
      double imntYield,
      double imntReturn,
      double stdDev,
      double peRatio) {
    return MarketDataProto.Instrument.newBuilder()
        .setTicker(
            MarketDataProto.Ticker.newBuilder()
                .setSymbol(symbol)
                .addData(MarketDataProto.Value.newBuilder().setPrice(capital).build())
                .build())
        .setBeta(beta)
        .setDividendYield(imntYield)
        .putMetaData("return", String.valueOf(imntReturn))
        .putMetaData("std_dev", String.valueOf(stdDev))
        .putMetaData("pe_ratio", String.valueOf(peRatio))
        .build();
  }

  private Optional<Double> fetchLatestPrice(String imnt, int tDate) {
    int days = 11;
    LocalDate tLocalDate = DateFormatUtil.getLocalDate(tDate);

    while (days-- > 0) {
      Double val = tickerDataWarehouseService.getMarketData(imnt, tLocalDate);
      if (val != null) return Optional.of(val);
      tLocalDate = tLocalDate.minusDays(1);
    }
    return Optional.empty();
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
      String imnt,
      Double currentValuationPnL,
      Double investmentActual,
      Double pnlPercentage,
      Double qty) {}
}
