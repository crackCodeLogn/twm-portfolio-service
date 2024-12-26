package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.portfolio.warehouse.bank.BankAccountWarehouse;
import com.vv.personal.twm.portfolio.warehouse.bank.BankFixedDepositsWarehouse;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2024-12-26
 */
@Slf4j
@Getter
@Service
@RequiredArgsConstructor
public class CompleteBankDataService {

  private final BankAccountWarehouse bankAccountWarehouse;
  private final BankFixedDepositsWarehouse bankFixedDepositsWarehouse;

  public void populateBankAccounts(BankProto.BankAccounts cadBankAccounts) {
    cadBankAccounts.getAccountsList().forEach(bankAccountWarehouse::addBankAccount);
  }

  public void populateFixedDeposits(
      FixedDepositProto.FixedDepositList fixedDepositList, BankProto.CurrencyCode currencyCode) {
    fixedDepositList
        .getFixedDepositList()
        .forEach(deposit -> bankFixedDepositsWarehouse.addFixedDeposit(deposit, currencyCode));
  }
}
