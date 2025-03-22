package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import java.util.OptionalDouble;
import java.util.TreeMap;

/**
 * @author Vivek
 * @since 2025-03-22
 */
public interface CompleteBankDataService {

  void load();

  void clear();

  OptionalDouble getNetBankAccountBalanceForCurrency(BankProto.CurrencyCode ccy);

  OptionalDouble getOtherNetBalanceForCurrency(BankProto.CurrencyCode ccy);

  FixedDepositProto.FixedDepositList getGicExpiries(BankProto.CurrencyCode currency);

  TreeMap<Integer, Double> getCumulativeDateAmountGicMap();
}
