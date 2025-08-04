package com.vv.personal.twm.portfolio.service.impl;

import com.google.common.collect.ImmutableMap;
import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.NetWorthBreakDownKey;
import com.vv.personal.twm.portfolio.service.CentralDataPointService;
import com.vv.personal.twm.portfolio.service.CompleteBankDataService;
import com.vv.personal.twm.portfolio.service.CompleteMarketDataService;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2025-02-09
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class CentralDataPointServiceImpl implements CentralDataPointService {

  private final CompleteBankDataService completeBankDataService;
  private final CompleteMarketDataService completeMarketDataService;

  @Override
  public Map<String, Double> getLatestTotalNetWorthBreakDown(BankProto.CurrencyCode ccy) {
    double marketNetWorth = getLatestMarketNetWorth().orElse(0.0);
    double gicNetWorth = 0.0;
    try {
      gicNetWorth = getGicValuations(ccy).lastEntry().getValue();
    } catch (Exception e) {
      log.warn("error while calculating gic net worth", e);
    }
    double bankNetWorth = getLatestBankNetWorth(ccy).orElse(0.0);
    double otherWorth = getLatestOtherNetWorth(ccy).orElse(0.0);
    double netWorth = marketNetWorth + gicNetWorth + bankNetWorth + otherWorth;
    return ImmutableMap.<String, Double>builder()
        .put(NetWorthBreakDownKey.NET_WORTH.getKey(), netWorth)
        .put(NetWorthBreakDownKey.MARKET.getKey(), marketNetWorth)
        .put(NetWorthBreakDownKey.GIC.getKey(), gicNetWorth)
        .put(NetWorthBreakDownKey.BANK.getKey(), bankNetWorth)
        .put(NetWorthBreakDownKey.OTHER.getKey(), otherWorth)
        .build();
  }

  @Override
  public OptionalDouble getLatestTotalNetWorth(BankProto.CurrencyCode ccy) {
    return OptionalDouble.of(
        getLatestTotalNetWorthBreakDown(ccy).get(NetWorthBreakDownKey.NET_WORTH.getKey()));
  }

  @Override
  public OptionalDouble getLatestBankNetWorth(BankProto.CurrencyCode ccy) {
    return completeBankDataService.getNetBankAccountBalanceForCurrency(ccy);
  }

  @Override
  public OptionalDouble getLatestOtherNetWorth(BankProto.CurrencyCode ccy) {
    return completeBankDataService.getOtherNetBalanceForCurrency(ccy);
  }

  @Override
  public OptionalDouble getLatestMarketNetWorth() {
    return OptionalDouble.of(
        completeMarketDataService.getLatestCombinedCumulativePnL()
            + completeMarketDataService.getLatestTotalInvestmentAmount());
  }

  @Override
  public FixedDepositProto.FixedDepositList getGicExpiries(BankProto.CurrencyCode currency) {
    return completeBankDataService.getGicExpiries(currency);
  }

  @Override
  public TreeMap<Integer, Double> getGicValuations(BankProto.CurrencyCode currency) {
    return completeBankDataService.getCumulativeDateAmountGicMap();
  }

  @Override
  public TreeMap<Integer, Double> getMarketValuations(MarketDataProto.AccountType accountType) {
    TreeMap<Integer, Double> marketValuations = new TreeMap<>();
    TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> combinedDatePnLCumulativeMap =
        completeMarketDataService.getCombinedDatePnLCumulativeMap();
    combinedDatePnLCumulativeMap.forEach(
        (date, accountValueMap) -> marketValuations.put(date, accountValueMap.get(accountType)));
    return marketValuations;
  }

  @Override
  public TreeMap<Integer, Double> getMarketValuations() {
    TreeMap<Integer, Double> marketValuations = new TreeMap<>();
    TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> combinedDatePnLCumulativeMap =
        completeMarketDataService.getCombinedDatePnLCumulativeMap();
    combinedDatePnLCumulativeMap.forEach(
        (date, accountValueMap) -> {
          double value = 0.0;
          for (Double accountValue : accountValueMap.values()) value += accountValue;
          marketValuations.put(date, value);
        });
    return marketValuations;
  }

  @Override
  public Map<String, Double> getCumulativeImntDividendValuations(
      MarketDataProto.AccountType accountType) {
    Map<String, Double> cumulativeImntDividendValuations = new HashMap<>();

    Map<String, Map<MarketDataProto.AccountType, Double>> cumulativeImntAccountTypeDividendMap =
        completeMarketDataService.getCumulativeImntAccountTypeDividendMap();
    cumulativeImntAccountTypeDividendMap.forEach(
        (imnt, accountValueMap) -> {
          if (accountValueMap != null && accountValueMap.get(accountType) != null) {
            cumulativeImntDividendValuations.put(imnt, accountValueMap.get(accountType));
          }
        });
    return cumulativeImntDividendValuations;
  }
}
