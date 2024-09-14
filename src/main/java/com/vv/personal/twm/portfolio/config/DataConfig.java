package com.vv.personal.twm.portfolio.config;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.market.warehouse.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.model.market.AdjustedCostBase2;
import com.vv.personal.twm.portfolio.model.market.OrderDirection;
import com.vv.personal.twm.portfolio.model.market.warehouse.PortfolioData;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataEngineFeign;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Slf4j
@Configuration
@AllArgsConstructor
public class DataConfig {

  private final MarketDataEngineFeign marketDataEngineFeign;
  private final TickerDataWarehouseService tickerDataWarehouseService;

  @Qualifier("portfolio-b")
  @Bean
  public PortfolioData extractBoughtPortfolioData() {
    MarketDataProto.Portfolio portfolio =
        marketDataEngineFeign.getPortfolioData(OrderDirection.BUY.getDirection());
    log.debug("result => {}", portfolio);
    return new PortfolioData(portfolio);
  }

  @Qualifier("portfolio-s")
  @Bean
  public PortfolioData extractSoldPortfolioData() {
    MarketDataProto.Portfolio portfolio =
        marketDataEngineFeign.getPortfolioData(OrderDirection.SELL.getDirection());
    log.debug("result => {}", portfolio);
    return new PortfolioData(portfolio);
  }

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
  }

  @Bean
  // todo - think about updating this from the sold portfolio as well
  public AdjustedCostBase2 createAdjustedCostBase() {
    log.info("Initiating creation of adjusted cost base data");
    PortfolioData portfolioData = extractBoughtPortfolioData();
    AdjustedCostBase2 adjustedCostBase2 = new AdjustedCostBase2();

    portfolioData.getPortfolio().getInstrumentsList().forEach(adjustedCostBase2::addBlock);
    adjustedCostBase2
        .getInstruments()
        .forEach(
            instrument ->
                adjustedCostBase2
                    .getAccountTypes()
                    .forEach(
                        accountType ->
                            //                                        log.info("{}:{} -> {} =>
                            // [{}]", instrument, accountType,
                            //
                            // adjustedCostBase.getAdjustedCost(instrument, accountType),
                            //
                            // adjustedCostBase.getInvestmentData(instrument, accountType)); //
                            // verbose

                            log.info(
                                "{}:{} -> {}",
                                instrument,
                                accountType,
                                adjustedCostBase2.getAdjustedCost(instrument, accountType))));
    log.info("Completed creation of adjusted cost base data");
    return adjustedCostBase2;
  }
}
