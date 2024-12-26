package com.vv.personal.twm.portfolio.warehouse.bank;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * @author Vivek
 * @since 2024-12-23
 */
public interface BankAccountWarehouse {

  void addBankAccount(BankProto.BankAccount bankAccount);

  void updateBankAccount(String id, BankProto.BankAccount bankAccount);

  Optional<BankProto.BankAccount> getBankAccount(String id);

  OptionalDouble getBankAccountBalance(String id);

  OptionalDouble getNetBankAccountBalanceForCurrency(BankProto.CurrencyCode currencyCode);

  void clear();
}
