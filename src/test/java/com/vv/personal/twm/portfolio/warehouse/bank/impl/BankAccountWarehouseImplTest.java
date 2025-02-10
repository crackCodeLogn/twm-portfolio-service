package com.vv.personal.twm.portfolio.warehouse.bank.impl;

import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Sets;
import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
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
        4, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());

    Optional<BankProto.BankAccount> optionalBankAccount;
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid1");
    assertTrue(optionalBankAccount.isPresent());
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid2");
    assertTrue(optionalBankAccount.isPresent());
    optionalBankAccount = bankAccountWarehouse.getBankAccount("uuid3");
    assertTrue(optionalBankAccount.isPresent());
    assertEquals(5, bankAccountWarehouse.getBankAccountsMap().size());
  }

  @Test
  void testUpdateBankAccountAndGetBankAccountBalance() {
    generateTestBankAccounts()
        .getAccountsList()
        .forEach(bankAccount -> bankAccountWarehouse.addBankAccount(bankAccount));
    assertEquals(
        1, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.INR).size());
    assertEquals(
        4, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());

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
        4, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());

    // shifting uuid2 from INR to CAD
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
        5, bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.CAD).size());
    assertTrue(
        bankAccountWarehouse.getCcyBankAccountIdsMap().get(BankProto.CurrencyCode.INR).isEmpty());
  }

  @Test
  void testGetNetBankAccountBalanceForCurrency() {
    generateTestBankAccounts()
        .getAccountsList()
        .forEach(bankAccount -> bankAccountWarehouse.addBankAccount(bankAccount));

    Set<BankProto.BankAccountType> bankAccountTypes;
    OptionalDouble optionalDouble;

    bankAccountTypes = Sets.newHashSet(BankProto.BankAccountType.CASH_R);
    // test normal positive filter, i.e. get balance from accounts whose tags match the bank account
    // types
    optionalDouble =
        bankAccountWarehouse.getNetBankAccountBalanceForCurrency(
            BankProto.CurrencyCode.CAD, bankAccountTypes, true);
    assertTrue(optionalDouble.isPresent());
    assertEquals(7501.00, optionalDouble.getAsDouble(), DELTA_PRECISION);

    // test inverted filter, i.e. get balance from accounts whose tags do not contain the bank
    // account types
    bankAccountTypes =
        Sets.newHashSet(BankProto.BankAccountType.MKT, BankProto.BankAccountType.CASH_R);
    optionalDouble =
        bankAccountWarehouse.getNetBankAccountBalanceForCurrency(
            BankProto.CurrencyCode.CAD, bankAccountTypes, false);
    assertTrue(optionalDouble.isPresent());
    assertEquals(12002, optionalDouble.getAsDouble(), DELTA_PRECISION);

    // test no filters, i.e. get full sum for CAD
    bankAccountTypes.clear();
    optionalDouble =
        bankAccountWarehouse.getNetBankAccountBalanceForCurrency(
            BankProto.CurrencyCode.CAD, bankAccountTypes, false);
    assertTrue(optionalDouble.isPresent());
    assertEquals(19604, optionalDouble.getAsDouble(), DELTA_PRECISION);

    // test no filters, i.e. get full sum for INR
    bankAccountTypes.clear();
    optionalDouble =
        bankAccountWarehouse.getNetBankAccountBalanceForCurrency(
            BankProto.CurrencyCode.INR, bankAccountTypes, false);
    assertTrue(optionalDouble.isPresent());
    assertEquals(5555.44, optionalDouble.getAsDouble(), DELTA_PRECISION);
  }

  private BankProto.BankAccounts generateTestBankAccounts() {
    return BankProto.BankAccounts.newBuilder()
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid1")
                .setCcy(BankProto.CurrencyCode.CAD)
                .setBalance(101.00)
                .addBankAccountTypes(BankProto.BankAccountType.MKT)
                .addBankAccountTypes(BankProto.BankAccountType.NR)
                .setExternalId("ext1")) // mimic the sha512 hash
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid2")
                .setCcy(BankProto.CurrencyCode.INR)
                .setBalance(5555.44)
                .setExternalId("ext2"))
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid3")
                .setCcy(BankProto.CurrencyCode.CAD)
                .setBalance(2501.00)
                .setExternalId("ext3"))
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid4")
                .setCcy(BankProto.CurrencyCode.CAD)
                .setBalance(7501.00)
                .addBankAccountTypes(BankProto.BankAccountType.CASH_R)
                .setExternalId("ext4"))
        .addAccounts(
            BankProto.BankAccount.newBuilder()
                .setId("uuid5")
                .setCcy(BankProto.CurrencyCode.CAD)
                .setBalance(9501.00)
                .setExternalId("ext5"))
        .build();
  }
}
