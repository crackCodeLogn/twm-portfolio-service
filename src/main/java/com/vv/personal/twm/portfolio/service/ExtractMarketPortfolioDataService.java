package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.warehouse.PortfolioData;

/**
 * @author Vivek
 * @since 2025-03-22
 */
public interface ExtractMarketPortfolioDataService {

  PortfolioData extractMarketPortfolioData(MarketDataProto.Direction direction);

  PortfolioData extractMarketPortfolioDividendData(MarketDataProto.AccountType accountType);
}
