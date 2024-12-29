package com.vv.personal.twm.portfolio.config;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.warehouse.PortfolioData;
import com.vv.personal.twm.portfolio.remote.feign.BankCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market_transactions.DownloadMarketTransactions;
import com.vv.personal.twm.portfolio.service.CompleteBankDataService;
import com.vv.personal.twm.portfolio.service.CompleteMarketDataService;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.service.impl.TickerDataWarehouseServiceImpl;
import com.vv.personal.twm.portfolio.warehouse.bank.BankAccountWarehouse;
import com.vv.personal.twm.portfolio.warehouse.bank.BankFixedDepositsWarehouse;
import com.vv.personal.twm.portfolio.warehouse.bank.impl.BankAccountWarehouseImpl;
import com.vv.personal.twm.portfolio.warehouse.bank.impl.BankFixedDepositsWarehouseImpl;
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
  private final BankCrdbServiceFeign bankCrdbServiceFeign;

  @Bean
  public TickerDataWarehouse tickerDataWarehouse() {
    return new TickerDataWarehouseImpl();
  }

  @Bean
  public BankAccountWarehouse bankAccountWarehouse() {
    return new BankAccountWarehouseImpl();
  }

  @Bean
  public BankFixedDepositsWarehouse bankFixedDepositsWarehouse() {
    return new BankFixedDepositsWarehouseImpl();
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
    return extractPortfolioData(
        MarketDataProto.AccountType.TFSA, fileLocationConfig.getMarketTransactionsDivTfsa());
  }

  @Qualifier("dividends-nr")
  @Bean
  public PortfolioData extractNrDividendsData() {
    return extractPortfolioData(
        MarketDataProto.AccountType.NR, fileLocationConfig.getMarketTransactionsDivNr());
  }

  @Qualifier("dividends-fhsa")
  @Bean
  public PortfolioData extractFhsaDividendsData() {
    return extractPortfolioData(
        MarketDataProto.AccountType.FHSA, fileLocationConfig.getMarketTransactionsDivFhsa());
  }

  @Bean
  public CompleteBankDataService completeBankDataService() {
    CompleteBankDataService completeBankDataService =
        new CompleteBankDataService(bankAccountWarehouse(), bankFixedDepositsWarehouse());

    log.info("Initiating complete bank data load");
    StopWatch stopWatch = StopWatch.createStarted();
    // get CAD based bank accounts only, for now
    FixedDepositProto.FilterBy filterByCcyField = FixedDepositProto.FilterBy.CCY;
    BankProto.CurrencyCode currencyCodeCad = BankProto.CurrencyCode.CAD;

    BankProto.BankAccounts cadBankAccounts =
        bankCrdbServiceFeign.getBankAccounts(filterByCcyField.name(), currencyCodeCad.name());
    if (cadBankAccounts != null) completeBankDataService.populateBankAccounts(cadBankAccounts);

    FixedDepositProto.FixedDepositList fixedDepositList =
        bankCrdbServiceFeign.getFixedDeposits(filterByCcyField.name(), currencyCodeCad.name());
    if (fixedDepositList != null)
      completeBankDataService.populateFixedDeposits(fixedDepositList, currencyCodeCad);

    stopWatch.stop();
    log.info("Complete bank data load finished in {}ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
    return completeBankDataService;
  }

  @Bean
  public CompleteMarketDataService completeMarketDataService() {
    OutdatedSymbols outdatedSymbols = outdatedSymbols();

    CompleteMarketDataService marketDataService = new CompleteMarketDataService();
    marketDataService.setOutdatedSymbols(outdatedSymbols);
    log.info("Initiating complete market data load");
    StopWatch stopWatch = StopWatch.createStarted();
    marketDataService.populate(
        extractBoughtPortfolioData().getPortfolio()); // first populate the buy side
    marketDataService.populate(
        extractSoldPortfolioData().getPortfolio()); // then populate the sell side
    marketDataService.computeAcb(); // compute the ACB once all the data has been populated

    MarketDataProto.Portfolio tfsaDividends = extractTfsaDividendsData().getPortfolio();
    MarketDataProto.Portfolio nrDividends = extractNrDividendsData().getPortfolio();
    MarketDataProto.Portfolio fhsaDividends = extractFhsaDividendsData().getPortfolio();
    marketDataService.populateDividends(tfsaDividends);
    marketDataService.populateDividends(nrDividends);
    marketDataService.populateDividends(fhsaDividends);

    TickerDataWarehouseService tickerDataWarehouseService = tickerDataWarehouseService();
    // load analysis data for imnts which are bought
    tickerDataWarehouseService.loadAnalysisDataForInstruments(marketDataService.getInstruments());
    marketDataService.setTickerDataWarehouseService(tickerDataWarehouseService);
    marketDataService.computePnL();

    stopWatch.stop();
    log.info(
        "Complete market data load finished in {}ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
    return marketDataService;
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

  private PortfolioData extractPortfolioData(
      MarketDataProto.AccountType accountType, String fileLocation) {
    String accountTypeStr = accountType.name();
    StopWatch stopwatch = StopWatch.createStarted();
    MarketDataProto.Portfolio dbPortfolio =
        marketDataCrdbServiceFeign
            .getDividends(accountTypeStr)
            .orElse(MarketDataProto.Portfolio.newBuilder().build());
    MarketDataProto.Portfolio dividends =
        DownloadMarketTransactions.downloadMarketDividendTransactions(fileLocation, accountType);
    Map<String, MarketDataProto.Instrument> dbTransactions = getTransactionsMap(dbPortfolio);
    Map<String, MarketDataProto.Instrument> actualTransactions = getTransactionsMap(dividends);

    dbTransactions.keySet().forEach(actualTransactions::remove);
    MarketDataProto.Portfolio.Builder newTransactions = MarketDataProto.Portfolio.newBuilder();
    actualTransactions.values().forEach(newTransactions::addInstruments);
    if (newTransactions.getInstrumentsCount() > 0) {
      log.info(
          "Pushing {} new {} dividends to db",
          newTransactions.getInstrumentsCount(),
          accountTypeStr);
      String result = marketDataCrdbServiceFeign.postTransactions(newTransactions.build());
      log.info(
          "Result of posting {} {} dividends to db: {}",
          newTransactions.getInstrumentsCount(),
          accountTypeStr,
          result);
    } else {
      log.info("No new {} dividends to push to db", accountTypeStr);
    }

    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} {} dividends in {}s",
        dividends.getInstrumentsCount(),
        accountTypeStr,
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(dividends);
  }
}
