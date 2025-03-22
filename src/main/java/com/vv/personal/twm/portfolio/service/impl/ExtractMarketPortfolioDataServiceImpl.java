package com.vv.personal.twm.portfolio.service.impl;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.FileLocationConfig;
import com.vv.personal.twm.portfolio.model.market.warehouse.PortfolioData;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.market.transactions.DownloadMarketTransactions;
import com.vv.personal.twm.portfolio.service.ExtractMarketPortfolioDataService;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2025-03-22
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class ExtractMarketPortfolioDataServiceImpl implements ExtractMarketPortfolioDataService {

  private final MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;
  private final FileLocationConfig fileLocationConfig;

  @Override
  public PortfolioData extractMarketPortfolioData(MarketDataProto.Direction direction) {
    log.info("Initiating extraction of market portfolio data for direction {}", direction);
    StopWatch stopwatch = StopWatch.createStarted();
    MarketDataProto.Portfolio dbPortfolio =
        marketDataCrdbServiceFeign
            .getTransactions(direction.name())
            .orElse(MarketDataProto.Portfolio.newBuilder().build());
    MarketDataProto.Portfolio actualPortfolio =
        DownloadMarketTransactions.downloadMarketTransactions(
            getFileLocationConfig(direction), direction);

    Map<String, MarketDataProto.Instrument> dbTransactions = getTransactionsMap(dbPortfolio);
    Map<String, MarketDataProto.Instrument> actualTransactions =
        getTransactionsMap(actualPortfolio);
    dbTransactions.keySet().forEach(actualTransactions::remove);
    MarketDataProto.Portfolio.Builder newTransactions = MarketDataProto.Portfolio.newBuilder();
    actualTransactions.values().forEach(newTransactions::addInstruments);

    if (newTransactions.getInstrumentsCount() > 0) {
      log.info(
          "Pushing {} new {} transactions to db", newTransactions.getInstrumentsCount(), direction);
      String result = marketDataCrdbServiceFeign.postTransactions(newTransactions.build());
      log.info(
          "Result of posting {} {} transactions to db: {}",
          newTransactions.getInstrumentsCount(),
          direction,
          result);
    } else {
      log.info("No new {} transactions to push to db", direction);
    }

    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} {} stocks in {}s",
        actualPortfolio.getInstrumentsCount(),
        direction,
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(actualPortfolio);
  }

  @Override
  public PortfolioData extractMarketPortfolioDividendData(MarketDataProto.AccountType accountType) {
    return extractPortfolioData(accountType, getFileLocationConfig(accountType));
  }

  private String getFileLocationConfig(MarketDataProto.Direction direction) {
    return switch (direction) {
      case BUY -> fileLocationConfig.getMarketTransactionsBuy();
      case SELL -> fileLocationConfig.getMarketTransactionsSell();
      default -> StringUtils.EMPTY;
    };
  }

  private String getFileLocationConfig(MarketDataProto.AccountType accountType) {
    return switch (accountType) {
      case TFSA -> fileLocationConfig.getMarketTransactionsDivTfsa();
      case NR -> fileLocationConfig.getMarketTransactionsDivNr();
      case FHSA -> fileLocationConfig.getMarketTransactionsDivFhsa();
      default -> StringUtils.EMPTY;
    };
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
