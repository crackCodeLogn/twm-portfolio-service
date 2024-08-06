package com.vv.personal.twm.mkt.config;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.mkt.market.warehouse.TickerDataWarehouse;
import com.vv.personal.twm.mkt.market.warehouse.holding.PortfolioData;
import com.vv.personal.twm.mkt.model.AdjustedCostBase;
import com.vv.personal.twm.mkt.remote.feign.MarketDataEngineFeign;
import com.vv.personal.twm.mkt.util.DateFormatUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;
import java.util.TreeSet;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Slf4j
@Configuration
@AllArgsConstructor
public class BeanSupplier {

    private final MarketDataEngineFeign marketDataEngineFeign;

    @Bean
    public PortfolioData extractPortfolioData() {
//        System.out.println(LocalDate.parse("11/28/2022", DATE_TIME_FORMATTER_INVESTMENT));
//        System.out.println(LocalDate.parse("12/9/2022", DATE_TIME_FORMATTER_INVESTMENT));
//        System.out.println(LocalDate.parse("1/13/2023", DATE_TIME_FORMATTER_INVESTMENT));
//        System.out.println(LocalDate.parse("2/2/2023", DATE_TIME_FORMATTER_INVESTMENT));

        //log.info("{}", marketDataEngineFeign.getTickerName("CA", "CM"));
        //log.info("{}", marketDataEngineFeign.getTickerSector("CA", "CM"));
        //log.info("{}", marketDataEngineFeign.getTickerDataWithoutCountryCode("CM.TO", "2023-11-01", "2023-11-24"));
        MarketDataProto.Portfolio portfolio = marketDataEngineFeign.getPortfolioData();
        log.info("result => {}", portfolio);
        return new PortfolioData(portfolio);
        //return null;
    }

    @Bean
    public TickerDataWarehouse createTickerDataWarehouse() {
        PortfolioData portfolioData = extractPortfolioData();
        LocalDate firstStartDate = LocalDate.of(2999, 12, 31);
        TreeSet<String> instruments = new TreeSet<>();

        for (MarketDataProto.Investment investment : portfolioData.getPortfolio().getInvestmentsList()) {
            instruments.add(investment.getTicker().getSymbol());

            LocalDate date = DateFormatUtil.getLocalDate(investment.getTicker().getData(0).getDate());
            if (date.isBefore(firstStartDate)) {
                firstStartDate = date;
            }
        }
        LocalDate endDate = LocalDate.now().plusDays(1);
        LocalDate startDateForAnalysis = endDate.minusYears(7);
        TickerDataWarehouse warehouse = new TickerDataWarehouse(marketDataEngineFeign, instruments, firstStartDate, endDate, startDateForAnalysis);
        warehouse.generateData();
        warehouse.setPortfolioData(portfolioData);
        return warehouse;
    }

    @Bean
    public AdjustedCostBase createAdjustedCostBase() {
        log.info("Initiating creation of adjusted cost base data");
        PortfolioData portfolioData = extractPortfolioData();
        AdjustedCostBase adjustedCostBase = new AdjustedCostBase();

        portfolioData.getPortfolio().getInvestmentsList().forEach(adjustedCostBase::addBlock);
        adjustedCostBase.getInstruments()
                .forEach(instrument ->
                        adjustedCostBase.getAccountTypes()
                                .forEach(accountType ->
//                                        log.info("{}:{} -> {} => [{}]", instrument, accountType,
//                                                adjustedCostBase.getAdjustedCost(instrument, accountType),
//                                                adjustedCostBase.getInvestmentData(instrument, accountType)); // verbose

                                                log.info("{}:{} -> {}", instrument, accountType, adjustedCostBase.getAdjustedCost(instrument, accountType))
                                ));
        log.info("Completed creation of adjusted cost base data");
        return adjustedCostBase;
    }

}