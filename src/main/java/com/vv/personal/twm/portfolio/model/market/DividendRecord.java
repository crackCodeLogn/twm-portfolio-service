package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;

/**
 * @author Vivek
 * @since 2024-12-26
 */
public record DividendRecord(
    String symbol,
    int date,
    double dividend,
    String orderId,
    MarketDataProto.AccountType accountType) {}
