package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.TickerDataWarehouseConfig;
import com.vv.personal.twm.portfolio.market.warehouse.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.model.market.warehouse.PortfolioData;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataEngineFeign;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import java.time.LocalDate;
import java.util.TreeSet;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2024-09-13
 */
@AllArgsConstructor
@Service
public class TickerDataWarehouseService {

  private final TickerDataWarehouseConfig tickerDataWarehouseConfig;
  private final MarketDataEngineFeign marketDataEngineFeign;

  public TickerDataWarehouse getTickerDataWarehouse(PortfolioData portfolioData) {
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

    if (tickerDataWarehouseConfig.isLoad()) warehouse.generateData();

    warehouse.setPortfolioData(portfolioData);
    return warehouse;
  }
}
