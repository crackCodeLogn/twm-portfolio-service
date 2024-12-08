package com.vv.personal.twm.portfolio.config;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.market.warehouse.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.market.warehouse.impl.TickerDataWarehouseImpl;
import com.vv.personal.twm.portfolio.model.market.CompleteMarketData;
import com.vv.personal.twm.portfolio.model.market.OrderDirection;
import com.vv.personal.twm.portfolio.model.market.warehouse.PortfolioData;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.service.impl.TickerDataWarehouseServiceImpl;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
    MarketDataProto.Portfolio portfolio =
        marketDataPythonEngineFeign.getPortfolioData(OrderDirection.BUY.getDirection());
    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} bought stocks in {}s",
        portfolio.getInstrumentsCount(),
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(portfolio);
  }

  @Qualifier("portfolio-s")
  @Bean
  public PortfolioData extractSoldPortfolioData() {
    StopWatch stopwatch = StopWatch.createStarted();
    MarketDataProto.Portfolio portfolio =
        marketDataPythonEngineFeign.getPortfolioData(OrderDirection.SELL.getDirection());
    stopwatch.stop();
    log.info(
        "Loaded portfolio of {} sold stocks in {}s",
        portfolio.getInstrumentsCount(),
        stopwatch.getTime(TimeUnit.SECONDS));
    return new PortfolioData(portfolio);
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

  /*
  @Qualifier("ticker-dwh-b")
  @Lazy
  @Bean
  public TickerDataWarehouse createBoughtTickerDataWarehouse() {
    return tickerDataWarehouseService.getTickerDataWarehouse(extractBoughtPortfolioData());
  }

  @Qualifier("ticker-dwh-s")
  @Lazy
  @Bean
  public TickerDataWarehouse createSoldTickerDataWarehouse() {
    return tickerDataWarehouseService.getTickerDataWarehouse(extractSoldPortfolioData());
  }*/

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

    TickerDataWarehouseService tickerDataWarehouseService = tickerDataWarehouseService();
    // load analysis data for imnts which are bought
    tickerDataWarehouseService.loadAnalysisDataForInstruments(marketData.getInstruments());
    marketData.setTickerDataWarehouseService(tickerDataWarehouseService);
    marketData.computePnL(); // todo - uncomment to fire up pnl compute
    stopWatch.stop();
    log.info(
        "Completed market data load completed in {}ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
    return marketData;
  }

  @Bean
  public OutdatedSymbols outdatedSymbols() {
    OutdatedSymbols outdatedSymbols = new OutdatedSymbols();
    outdatedSymbols.load("src/main/resources/outdated-symbols.csv");
    return outdatedSymbols;
  }
}
