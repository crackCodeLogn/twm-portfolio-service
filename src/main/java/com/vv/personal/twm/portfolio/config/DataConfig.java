package com.vv.personal.twm.portfolio.config;

import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.remote.feign.BankCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.CalcServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market.outdated.OutdatedSymbols;
import com.vv.personal.twm.portfolio.service.CentralDataPointService;
import com.vv.personal.twm.portfolio.service.CompleteBankDataService;
import com.vv.personal.twm.portfolio.service.CompleteMarketDataService;
import com.vv.personal.twm.portfolio.service.ComputeStatisticsService;
import com.vv.personal.twm.portfolio.service.ExtractMarketPortfolioDataService;
import com.vv.personal.twm.portfolio.service.ProgressTrackerService;
import com.vv.personal.twm.portfolio.service.ReloadService;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.service.impl.CentralDataPointServiceImpl;
import com.vv.personal.twm.portfolio.service.impl.CompleteBankDataServiceImpl;
import com.vv.personal.twm.portfolio.service.impl.CompleteMarketDataServiceImpl;
import com.vv.personal.twm.portfolio.service.impl.ComputeStatisticsServiceImpl;
import com.vv.personal.twm.portfolio.service.impl.ExtractMarketPortfolioDataServiceImpl;
import com.vv.personal.twm.portfolio.service.impl.ProgressTrackerServiceImpl;
import com.vv.personal.twm.portfolio.service.impl.ReloadServiceImpl;
import com.vv.personal.twm.portfolio.service.impl.TickerDataWarehouseServiceImpl;
import com.vv.personal.twm.portfolio.warehouse.bank.BankAccountWarehouse;
import com.vv.personal.twm.portfolio.warehouse.bank.BankFixedDepositsWarehouse;
import com.vv.personal.twm.portfolio.warehouse.bank.impl.BankAccountWarehouseImpl;
import com.vv.personal.twm.portfolio.warehouse.bank.impl.BankFixedDepositsWarehouseImpl;
import com.vv.personal.twm.portfolio.warehouse.market.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.warehouse.market.impl.TickerDataWarehouseImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
  private final CalcServiceFeign calcServiceFeign;

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

  @Bean
  public DateLocalDateCache dateLocalDateCache() {
    return new DateLocalDateCache();
  }

  @Bean
  public ExtractMarketPortfolioDataService extractMarketPortfolioDataService() {
    return new ExtractMarketPortfolioDataServiceImpl(
        marketDataCrdbServiceFeign, fileLocationConfig);
  }

  @Bean
  public TickerDataWarehouseService tickerDataWarehouseService() {
    TickerDataWarehouseService tickerDataWarehouseService =
        new TickerDataWarehouseServiceImpl(
            tickerDataWarehouseConfig,
            marketDataPythonEngineFeign,
            marketDataCrdbServiceFeign,
            tickerDataWarehouse(),
            outdatedSymbols());
    tickerDataWarehouseService.loadBenchmarkData();
    return tickerDataWarehouseService;
  }

  @Bean
  public CompleteBankDataService completeBankDataService() {
    return new CompleteBankDataServiceImpl(
        bankAccountWarehouse(),
        bankFixedDepositsWarehouse(),
        bankCrdbServiceFeign,
        calcServiceFeign,
        dateLocalDateCache(),
        progressTrackerService());
  }

  @Bean(destroyMethod = "shutdown")
  public CompleteMarketDataService completeMarketDataService() {
    CompleteMarketDataService marketDataService =
        new CompleteMarketDataServiceImpl(
            dateLocalDateCache(),
            extractMarketPortfolioDataService(),
            tickerDataWarehouseService(),
            marketDataPythonEngineFeign,
            progressTrackerService(),
            computeStatisticsService(),
            marketDataCrdbServiceFeign);
    marketDataService.setOutdatedSymbols(outdatedSymbols());
    return marketDataService;
  }

  @Bean
  public ComputeStatisticsService computeStatisticsService() {
    return new ComputeStatisticsServiceImpl(tickerDataWarehouseService());
  }

  @Bean
  public CentralDataPointService centralDataPointService() {
    return new CentralDataPointServiceImpl(completeBankDataService(), completeMarketDataService());
  }

  @Bean
  public ProgressTrackerService progressTrackerService() {
    return new ProgressTrackerServiceImpl();
  }

  @Bean
  public OutdatedSymbols outdatedSymbols() {
    OutdatedSymbols outdatedSymbols = new OutdatedSymbols();
    outdatedSymbols.load(fileLocationConfig.getOutdatedSymbols());
    return outdatedSymbols;
  }

  @Bean
  public ReloadService reloadService() {
    return new ReloadServiceImpl(completeMarketDataService(), completeBankDataService());
  }
}
