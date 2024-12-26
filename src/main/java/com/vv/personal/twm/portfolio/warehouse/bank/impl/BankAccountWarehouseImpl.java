package com.vv.personal.twm.portfolio.warehouse.bank.impl;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.portfolio.warehouse.bank.BankAccountWarehouse;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author Vivek
 * @since 2024-12-25
 */
@Slf4j
@Component
public class BankAccountWarehouseImpl implements BankAccountWarehouse {

  private final Map<String, BankProto.BankAccount>
      bankAccountsMap; // hold bank account id x bank account proto
  private final Map<BankProto.CurrencyCode, Set<String>>
      ccyBankAccountIdsMap; // hold ccy x set of account ids having that ccy as their denomination

  public BankAccountWarehouseImpl() {
    this.ccyBankAccountIdsMap = new ConcurrentHashMap<>();
    this.bankAccountsMap = new ConcurrentHashMap<>();
  }

  @Override
  public void addBankAccount(BankProto.BankAccount bankAccount) {
    String id = bankAccount.getId();
    if (!bankAccountsMap.containsKey(id)) {
      bankAccountsMap.put(id, bankAccount);
      ccyBankAccountIdsMap.computeIfAbsent(bankAccount.getCcy(), k -> new HashSet<>()).add(id);
    } else { // upsert
      updateBankAccount(id, bankAccount);
    }
  }

  @Override
  public void updateBankAccount(String id, BankProto.BankAccount bankAccount) {
    if (bankAccountsMap.containsKey(id)) {
      BankProto.BankAccount olderBankAccount = bankAccountsMap.put(id, bankAccount);

      if (olderBankAccount != null && olderBankAccount.getCcy() != bankAccount.getCcy()) {
        log.warn(
            "Observing a currency change event for bank account with id: {} => {} to {}",
            id,
            olderBankAccount.getCcy(),
            bankAccount.getCcy());

        ccyBankAccountIdsMap.get(olderBankAccount.getCcy()).remove(id);
        ccyBankAccountIdsMap.get(bankAccount.getCcy()).add(id);
      }
    } else {
      log.warn("Bank account with id {} not found", id);
    }
  }

  @Override
  public Optional<BankProto.BankAccount> getBankAccount(String id) {
    return bankAccountsMap.containsKey(id)
        ? Optional.of(bankAccountsMap.get(id))
        : Optional.empty();
  }

  @Override
  public OptionalDouble getBankAccountBalance(String id) {
    return getBankAccount(id)
        .map(account -> OptionalDouble.of(account.getBalance()))
        .orElseGet(OptionalDouble::empty);
  }

  @Override
  public OptionalDouble getNetBankAccountBalanceForCurrency(BankProto.CurrencyCode currencyCode) {
    return OptionalDouble.of(
        ccyBankAccountIdsMap.get(currencyCode).stream()
            .mapToDouble(id -> bankAccountsMap.get(id).getBalance())
            .sum());
  }

  @Override
  public void clear() {
    bankAccountsMap.clear();
    ccyBankAccountIdsMap.clear();
  }

  Map<String, BankProto.BankAccount> getBankAccountsMap() {
    return bankAccountsMap;
  }

  Map<BankProto.CurrencyCode, Set<String>> getCcyBankAccountIdsMap() {
    return ccyBankAccountIdsMap;
  }
}
