package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * @author Vivek
 * @since 2025-02-09
 */
public interface CentralDataPointService {

  /** Return the latest total net worth => market + bank. Current working currency => CAD */
  Map<String, Double> getLatestTotalNetWorthBreakDown(BankProto.CurrencyCode ccy);

  /** Return the latest total net worth => market + bank. Current working currency => CAD */
  OptionalDouble getLatestTotalNetWorth(BankProto.CurrencyCode ccy);

  /** Return the latest bank net worth for input currency */
  OptionalDouble getLatestBankNetWorth(BankProto.CurrencyCode ccy);

  /** Return the latest net other worth for input currency */
  OptionalDouble getLatestOtherNetWorth(BankProto.CurrencyCode ccy);

  /** Return the latest combined market net worth. Working currency => CAD */
  OptionalDouble getLatestMarketNetWorth();

  /** Return gic list in order of upcoming expiries for @param currency */
  FixedDepositProto.FixedDepositList getGicExpiries(BankProto.CurrencyCode currency);

  Map<Integer, Double> getGicValuations(BankProto.CurrencyCode currency);

  Map<Integer, Double> getMarketValuations(MarketDataProto.AccountType accountType);

  Map<Integer, Double> getMarketValuations();
}
