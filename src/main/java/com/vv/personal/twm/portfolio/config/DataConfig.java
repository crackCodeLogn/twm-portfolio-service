package com.vv.personal.twm.portfolio.config;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.CompleteMarketData;
import com.vv.personal.twm.portfolio.model.market.warehouse.PortfolioData;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market_transactions.DownloadMarketTransactions;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.service.impl.TickerDataWarehouseServiceImpl;
import com.vv.personal.twm.portfolio.warehouse.market.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.warehouse.market.impl.TickerDataWarehouseImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class DataConfig {

  private final FileLocationConfig fileLocationConfig;
  private final TickerDataWarehouseConfig tickerDataWarehouseConfig;
  private final MarketDataPythonEngineFeign marketDataPythonEngineFeign;
  private final MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;

  @Bean
  public TickerDataWarehouse tickerDataWarehouse() {
    return new TickerDataWarehouseImpl();
  }

  @Qualifier("portfolio-b")
  @Bean
  public PortfolioData extractBoughtPortfolioData() {
    StopWatch stopwatch = StopWatch.createStarted();
    MarketDataProto.Portfolio dbPortfolio =
        marketDataCrdbServiceFeign
            .getTransactions(MarketDataProto.Direction.BUY.name())
            .orElse(MarketDataProto.Portfolio.newBuilder().build());
    MarketDataProto.Portfolio actualPortfolio =
        DownloadMarketTransactions.downloadMarketTransactions(
            fileLocationConfig.getMarketTransactionsBuy(), MarketDataProto.Direction.BUY);
    Map<String, MarketDataProto.Instrument> dbTransactions = getTransactionsMap(dbPortfolio);
    Map<String, MarketDataProto.Instrument> actualTransactions =
        getTransactionsMap(actualPortfolio);

    dbTransactions.keySet().forEach(actualTransactions::remove);
    MarketDataProto.Portfolio.Builder newTransactions = MarketDataProto.Portfolio.newBuilder();
    actualTransactions.values().forEach(newTransactions::addInstruments);
    if (newTransactions.getInstrumentsCount() > 0) {
      log.info("Pushing {} new buy transactions to db", newTransactions.getInstrumentsCount());
      String result = marketDataCrdbServiceFeign.postTransactions(newTransactions.build());
      log.info(
          "Result of posting {} buy transactions to db: {}",
          newTransactions.getInstrumentsCount(),
          result);
    } else {
      log.info("No new buy transactions to push to db");
    }

    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} bought stocks in {}s",
        actualPortfolio.getInstrumentsCount(),
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(actualPortfolio);
  }

  @Qualifier("portfolio-s")
  @Bean
  public PortfolioData extractSoldPortfolioData() {
    StopWatch stopwatch = StopWatch.createStarted();
    MarketDataProto.Portfolio dbPortfolio =
        marketDataCrdbServiceFeign
            .getTransactions(MarketDataProto.Direction.SELL.name())
            .orElse(MarketDataProto.Portfolio.newBuilder().build());
    MarketDataProto.Portfolio actualPortfolio =
        DownloadMarketTransactions.downloadMarketTransactions(
            fileLocationConfig.getMarketTransactionsSell(), MarketDataProto.Direction.SELL);
    Map<String, MarketDataProto.Instrument> dbTransactions = getTransactionsMap(dbPortfolio);
    Map<String, MarketDataProto.Instrument> actualTransactions =
        getTransactionsMap(actualPortfolio);

    dbTransactions.keySet().forEach(actualTransactions::remove);
    MarketDataProto.Portfolio.Builder newTransactions = MarketDataProto.Portfolio.newBuilder();
    actualTransactions.values().forEach(newTransactions::addInstruments);
    if (newTransactions.getInstrumentsCount() > 0) {
      log.info("Pushing {} new sell transactions to db", newTransactions.getInstrumentsCount());
      String result = marketDataCrdbServiceFeign.postTransactions(newTransactions.build());
      log.info(
          "Result of posting {} sell transactions to db: {}",
          newTransactions.getInstrumentsCount(),
          result);
    } else {
      log.info("No new sell transactions to push to db");
    }

    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} sold stocks in {}s",
        actualPortfolio.getInstrumentsCount(),
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(actualPortfolio);
  }

  @Bean
  public TickerDataWarehouseService tickerDataWarehouseService() {
    TickerDataWarehouseService tickerDataWarehouseService =
        new TickerDataWarehouseServiceImpl(
            tickerDataWarehouseConfig,
            marketDataPythonEngineFeign,
            marketDataCrdbServiceFeign,
            tickerDataWarehouse());
    tickerDataWarehouseService.loadBenchmarkData();
    return tickerDataWarehouseService;
  }

  @Qualifier("dividends-tfsa")
  @Bean
  public PortfolioData extractTfsaDividendsData() {
    StopWatch stopwatch = StopWatch.createStarted();
    MarketDataProto.Portfolio dbPortfolio =
        marketDataCrdbServiceFeign
            .getDividends(MarketDataProto.AccountType.TFSA.name())
            .orElse(MarketDataProto.Portfolio.newBuilder().build());
    MarketDataProto.Portfolio tfsaDividends =
        DownloadMarketTransactions.downloadMarketDividendTransactions(
            fileLocationConfig.getMarketTransactionsDivTfsa(), MarketDataProto.AccountType.TFSA);
    Map<String, MarketDataProto.Instrument> dbTransactions = getTransactionsMap(dbPortfolio);
    Map<String, MarketDataProto.Instrument> actualTransactions = getTransactionsMap(tfsaDividends);

    dbTransactions.keySet().forEach(actualTransactions::remove);
    MarketDataProto.Portfolio.Builder newTransactions = MarketDataProto.Portfolio.newBuilder();
    actualTransactions.values().forEach(newTransactions::addInstruments);
    if (newTransactions.getInstrumentsCount() > 0) {
      log.info("Pushing {} new tfsa dividends to db", newTransactions.getInstrumentsCount());
      String result = marketDataCrdbServiceFeign.postTransactions(newTransactions.build());
      log.info(
          "Result of posting {} tfsa dividends to db: {}",
          newTransactions.getInstrumentsCount(),
          result);
    } else {
      log.info("No new tfsa dividends to push to db");
    }

    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} tfsa dividends in {}s",
        tfsaDividends.getInstrumentsCount(),
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(tfsaDividends);
  }

  @Qualifier("dividends-nr")
  @Bean
  public PortfolioData extractNrDividendsData() {
    StopWatch stopwatch = StopWatch.createStarted();
    MarketDataProto.Portfolio dbPortfolio =
        marketDataCrdbServiceFeign
            .getDividends(MarketDataProto.AccountType.NR.name())
            .orElse(MarketDataProto.Portfolio.newBuilder().build());
    MarketDataProto.Portfolio nrDividends =
        DownloadMarketTransactions.downloadMarketDividendTransactions(
            fileLocationConfig.getMarketTransactionsDivNr(), MarketDataProto.AccountType.NR);
    Map<String, MarketDataProto.Instrument> dbTransactions = getTransactionsMap(dbPortfolio);
    Map<String, MarketDataProto.Instrument> actualTransactions = getTransactionsMap(nrDividends);

    dbTransactions.keySet().forEach(actualTransactions::remove);
    MarketDataProto.Portfolio.Builder newTransactions = MarketDataProto.Portfolio.newBuilder();
    actualTransactions.values().forEach(newTransactions::addInstruments);
    if (newTransactions.getInstrumentsCount() > 0) {
      log.info("Pushing {} new nr dividends to db", newTransactions.getInstrumentsCount());
      String result = marketDataCrdbServiceFeign.postTransactions(newTransactions.build());
      log.info(
          "Result of posting {} nr dividends to db: {}",
          newTransactions.getInstrumentsCount(),
          result);
    } else {
      log.info("No new nr dividends to push to db");
    }

    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} nr dividends in {}s",
        nrDividends.getInstrumentsCount(),
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(nrDividends);
  }

  @Bean
  public CompleteMarketData completeMarketData() {
    OutdatedSymbols outdatedSymbols = outdatedSymbols();

    CompleteMarketData marketData = new CompleteMarketData();
    marketData.setOutdatedSymbols(outdatedSymbols);
    log.info("Starting complete market data load");
    StopWatch stopWatch = StopWatch.createStarted();
    marketData.populate(extractBoughtPortfolioData().getPortfolio()); // first populate the buy side
    marketData.populate(extractSoldPortfolioData().getPortfolio()); // then populate the sell side
    marketData.computeAcb(); // compute the ACB once all the data has been populated

    MarketDataProto.Portfolio tfsaDividends = extractTfsaDividendsData().getPortfolio();
    MarketDataProto.Portfolio nrDividends = extractNrDividendsData().getPortfolio();
    marketData.populateDividends(tfsaDividends);
    marketData.populateDividends(nrDividends);

    TickerDataWarehouseService tickerDataWarehouseService = tickerDataWarehouseService();
    // load analysis data for imnts which are bought
    tickerDataWarehouseService.loadAnalysisDataForInstruments(marketData.getInstruments());
    marketData.setTickerDataWarehouseService(tickerDataWarehouseService);
    marketData.computePnL();

    stopWatch.stop();
    log.info(
        "Completed market data load completed in {}ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
    return marketData;
  }

  @Bean
  public OutdatedSymbols outdatedSymbols() {
    OutdatedSymbols outdatedSymbols = new OutdatedSymbols();
    outdatedSymbols.load(fileLocationConfig.getOutdatedSymbols());
    return outdatedSymbols;
  }

  private Map<String, MarketDataProto.Instrument> getTransactionsMap(
      MarketDataProto.Portfolio portfolio) {
    Map<String, MarketDataProto.Instrument> transactions = new HashMap<>();
    portfolio
        .getInstrumentsList()
        .forEach(
            instrument -> {
              String orderId = instrument.getMetaDataOrDefault("orderId", "");
              if (StringUtils.isEmpty(orderId)) {
                log.warn("Did not find orderId for {}", instrument);
              } else {
                if (transactions.containsKey(orderId)) {
                  log.error(
                      "OrderId {} already present in transactions. This should not happen, check data source",
                      orderId);
                } else {
                  transactions.put(orderId, instrument);
                }
              }
            });

    return transactions;
  }
}
