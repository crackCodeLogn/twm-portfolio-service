package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;

/**
 * @author Vivek
 * @since 2026-02-17
 */
public record SellRecord(
    String symbol,
    int date,
    MarketDataProto.AccountType accountType,
    double quantity,
    double pnL,
    double pricePerShare,
    ACB preAcb,
    double soldPrice,
    ACB currentAcb,
    boolean closingPosition) {}
