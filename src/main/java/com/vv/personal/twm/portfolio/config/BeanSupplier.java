package com.vv.personal.twm.portfolio.config;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.market.warehouse.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.market.warehouse.holding.PortfolioData;
import com.vv.personal.twm.portfolio.model.AdjustedCostBase2;
import com.vv.personal.twm.portfolio.model.OrderDirection;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataEngineFeign;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import java.time.LocalDate;
import java.util.TreeSet;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Slf4j
@Configuration
@AllArgsConstructor
public class BeanSupplier {

  private final MarketDataEngineFeign marketDataEngineFeign;

  @Qualifier("portfolio-b")
  @Bean
  public PortfolioData extractBoughtPortfolioData() {
    //        System.out.println(LocalDate.parse("11/28/2022", DATE_TIME_FORMATTER_INVESTMENT));
    //        System.out.println(LocalDate.parse("12/9/2022", DATE_TIME_FORMATTER_INVESTMENT));
    //        System.out.println(LocalDate.parse("1/13/2023", DATE_TIME_FORMATTER_INVESTMENT));
    //        System.out.println(LocalDate.parse("2/2/2023", DATE_TIME_FORMATTER_INVESTMENT));

    // log.info("{}", marketDataEngineFeign.getTickerName("CA", "CM"));
    // log.info("{}", marketDataEngineFeign.getTickerSector("CA", "CM"));
    // log.info("{}", marketDataEngineFeign.getTickerDataWithoutCountryCode("CM.TO", "2023-11-01",
    // "2023-11-24"));
    MarketDataProto.Portfolio portfolio =
        marketDataEngineFeign.getPortfolioData(OrderDirection.BUY.getDirection());
    log.info("result => {}", portfolio);
    return new PortfolioData(portfolio);
  }

  @Qualifier("portfolio-s")
  @Bean
  public PortfolioData extractSoldPortfolioData() {
    MarketDataProto.Portfolio portfolio =
        marketDataEngineFeign.getPortfolioData(OrderDirection.SELL.getDirection());
    log.info("result => {}", portfolio);
    return new PortfolioData(portfolio);
  }

  @Qualifier("ticker-dwh-b")
  @Bean
  public TickerDataWarehouse createBoughtTickerDataWarehouse() {
    return getTickerDataWarehouse(extractBoughtPortfolioData());
  }

  @Qualifier("ticker-dwh-s")
  @Bean
  public TickerDataWarehouse createSoldTickerDataWarehouse() {
    return getTickerDataWarehouse(extractSoldPortfolioData());
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

  private TickerDataWarehouse getTickerDataWarehouse(PortfolioData portfolioData) {
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
    TickerDataWarehouse warehouse =
        new TickerDataWarehouse(
            marketDataEngineFeign, instruments, firstStartDate, endDate, startDateForAnalysis);
    warehouse.generateData();
    warehouse.setPortfolioData(portfolioData);
    return warehouse;
  }
}
