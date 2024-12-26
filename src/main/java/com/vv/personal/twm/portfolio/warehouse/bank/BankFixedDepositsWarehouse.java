package com.vv.personal.twm.portfolio.warehouse.bank;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * @author Vivek
 * @since 2024-12-26
 */
public interface BankFixedDepositsWarehouse {

  void addFixedDeposit(
      FixedDepositProto.FixedDeposit fixedDeposit, BankProto.CurrencyCode currency);

  Optional<FixedDepositProto.FixedDeposit> getFixedDeposit(String id);

  Optional<FixedDepositProto.FixedDepositList> getFixedDepositsByCurrency(
      BankProto.CurrencyCode currency);

  Optional<FixedDepositProto.FixedDepositList> getAllFixedDeposits();

  Optional<FixedDepositProto.FixedDepositList> getActiveFixedDeposits(
      BankProto.CurrencyCode currency);

  OptionalDouble getNetActiveFixedDepositPrincipalAmountForCurrency(
      BankProto.CurrencyCode currencyCode);

  OptionalDouble getNetActiveFixedDepositExpectedAmountForCurrency(
      BankProto.CurrencyCode currencyCode);

  void clear();
}
