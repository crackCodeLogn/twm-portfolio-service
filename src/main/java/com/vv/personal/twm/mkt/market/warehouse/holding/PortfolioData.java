package com.vv.personal.twm.mkt.market.warehouse.holding;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import lombok.Getter;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Getter
public class PortfolioData {

    private final MarketDataProto.Portfolio portfolio;

    public PortfolioData(MarketDataProto.Portfolio portfolio) {
        this.portfolio = portfolio;
    }

    @Override
    public String toString() {
        return portfolio.toString();
    }

}
