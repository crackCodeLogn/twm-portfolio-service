package com.vv.personal.twm.portfolio.warehouse.bank.impl;

import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import java.util.Optional;
import java.util.OptionalDouble;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-12-25
 */
class BankAccountWarehouseImplTest {

  private BankAccountWarehouseImpl bankAccountWarehouse;

  @BeforeEach
  void setUp() {
    bankAccountWarehouse = new BankAccountWarehouseImpl();
  }

  @Test
  void testAddBankAccountAndGet() {
    generateTestBankAccounts()
        .getAccountsList()
        .forEach(bankAccount -> bankAccountWarehouse.addBankAccount(bankAccount));
    assertEquals(
        1, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.INR).size());
    assertEquals(
        2, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());

    Optional<BankProto.BankAccount> optionalBankAccount;
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid1");
    assertTrue(optionalBankAccount.isPresent());
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid2");
    assertTrue(optionalBankAccount.isPresent());
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid3");
    assertTrue(optionalBankAccount.isPresent());
    assertEquals(3, bankAccountWarehouse.getBankAccountsMap().size());
  }

  @Test
  void testUpdateBankAccountAndGetBankAccountBalance() {
    generateTestBankAccounts()
        .getAccountsList()
        .forEach(bankAccount -> bankAccountWarehouse.addBankAccount(bankAccount));
    assertEquals(
        1, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.INR).size());
    assertEquals(
        2, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());

    // no ccy change event
    BankProto.BankAccount updatedBankAccount =
        BankProto.BankAccount.newBuilder()
            .setId("uuid2")
            .setBalance(501.01)
            .setCcy(BankProto.CurrencyCode.INR)
            .build();
    bankAccountWarehouse.updateBankAccount("uuid2", updatedBankAccount);
    Optional<BankProto.BankAccount> optionalBankAccount;
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid2");
    assertTrue(optionalBankAccount.isPresent());
    assertEquals(501.01, optionalBankAccount.get().getBalance());
    assertEquals(
        1, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.INR).size());
    assertEquals(
        2, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());

    updatedBankAccount =
        BankProto.BankAccount.newBuilder()
            .setId("uuid2")
            .setCcy(BankProto.CurrencyCode.CAD)
            .setBalance(501.01)
            .build();
    bankAccountWarehouse.updateBankAccount("uuid2", updatedBankAccount);
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid2");
    assertTrue(optionalBankAccount.isPresent());
    assertEquals(501.01, optionalBankAccount.get().getBalance());
    assertEquals(BankProto.CurrencyCode.CAD, optionalBankAccount.get().getCcy());
    assertEquals(
        3, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());
    assertTrue(
        bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.INR).isEmpty());
  }

  @Test
  void testGetNetBankAccountBalanceForCurrency() {
    generateTestBankAccounts()
        .getAccountsList()
        .forEach(bankAccount -> bankAccountWarehouse.addBankAccount(bankAccount));
    assertEquals(
        1, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.INR).size());
    assertEquals(
        2, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());

    OptionalDouble optionalDouble =
        bankAccountWarehouse.getNetBankAccountBalanceForCurrency(BankProto.CurrencyCode.CAD);
    assertTrue(optionalDouble.isPresent());
    assertEquals(2602.00, optionalDouble.getAsDouble(), DELTA_PRECISION);

    optionalDouble =
        bankAccountWarehouse.getNetBankAccountBalanceForCurrency(BankProto.CurrencyCode.INR);
    assertTrue(optionalDouble.isPresent());
    assertEquals(5555.44, optionalDouble.getAsDouble(), DELTA_PRECISION);
  }

  private BankProto.BankAccounts generateTestBankAccounts() {
    return BankProto.BankAccounts.newBuilder()
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid1")
                .setCcy(BankProto.CurrencyCode.CAD)
                .setBalance(101.00))
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid2")
                .setCcy(BankProto.CurrencyCode.INR)
                .setBalance(5555.44))
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid3")
                .setCcy(BankProto.CurrencyCode.CAD)
                .setBalance(2501.00))
        .build();
  }
}
