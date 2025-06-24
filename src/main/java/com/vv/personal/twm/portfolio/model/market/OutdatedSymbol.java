package com.vv.personal.twm.portfolio.model.market;

/**
 * If outdateEndDate is -1, then ticker is to be outdated from outdateStartDate onwards, * otherwise
 * outdate status is from outdateStartDate to outdateEndDate
 *
 * @author Vivek
 * @since 2024-12-06
 */
public record OutdatedSymbol(String ticker, int outdateStartDate, int outdateEndDate) {}
