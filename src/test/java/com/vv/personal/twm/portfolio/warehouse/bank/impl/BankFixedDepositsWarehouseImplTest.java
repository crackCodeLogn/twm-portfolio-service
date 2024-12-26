package com.vv.personal.twm.portfolio.warehouse.bank.impl;

import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Sets;
import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-12-26
 */
class BankFixedDepositsWarehouseImplTest {

  private BankFixedDepositsWarehouseImpl bankFixedDepositsWarehouse;

  @BeforeEach
  void setUp() {
    bankFixedDepositsWarehouse = new BankFixedDepositsWarehouseImpl();
  }

  @Test
  void testAddFixedDepositAndGet() {
    generateCadFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.CAD));
    generateInrFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.INR));

    assertEquals(2, bankFixedDepositsWarehouse.getCcyFixedDepositIdsMap().size());
    assertEquals(
        3,
        bankFixedDepositsWarehouse
            .getCcyFixedDepositIdsMap()
            .get(BankProto.CurrencyCode.CAD)
            .size());
    assertEquals(
        1,
        bankFixedDepositsWarehouse
            .getCcyFixedDepositIdsMap()
            .get(BankProto.CurrencyCode.INR)
            .size());

    Optional<FixedDepositProto.FixedDeposit> optionalFixedDeposit =
        bankFixedDepositsWarehouse.getFixedDeposit("12345");
    assertTrue(optionalFixedDeposit.isPresent());
    assertEquals(1050, optionalFixedDeposit.get().getExpectedAmount(), DELTA_PRECISION);
  }

  @Test
  void testGetFixedDepositsByCurrency() {
    generateCadFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.CAD));
    generateInrFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.INR));

    Optional<FixedDepositProto.FixedDepositList> optionalFixedDepositList =
        bankFixedDepositsWarehouse.getFixedDepositsByCurrency(BankProto.CurrencyCode.CAD);
    assertTrue(optionalFixedDepositList.isPresent());
    assertEquals(3, optionalFixedDepositList.get().getFixedDepositCount());

    optionalFixedDepositList =
        bankFixedDepositsWarehouse.getFixedDepositsByCurrency(BankProto.CurrencyCode.INR);
    assertTrue(optionalFixedDepositList.isPresent());
    assertEquals(1, optionalFixedDepositList.get().getFixedDepositCount());
  }

  @Test
  void testGetAllFixedDeposits() {
    generateCadFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.CAD));
    generateInrFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.INR));

    Set<String> ids = Sets.newHashSet("12345", "54678", "d43r23", "99999");
    Optional<FixedDepositProto.FixedDepositList> optionalFixedDepositList =
        bankFixedDepositsWarehouse.getAllFixedDeposits();
    assertTrue(optionalFixedDepositList.isPresent());
    assertEquals(4, optionalFixedDepositList.get().getFixedDepositCount());
    optionalFixedDepositList
        .get()
        .getFixedDepositList()
        .forEach(fd -> assertTrue(ids.contains(fd.getFdNumber())));
  }

  @Test
  void testGetActiveFixedDeposits() {
    generateCadFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.CAD));
    generateInrFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.INR));

    Optional<FixedDepositProto.FixedDepositList> optionalFixedDepositList =
        bankFixedDepositsWarehouse.getActiveFixedDeposits(BankProto.CurrencyCode.CAD);
    assertTrue(optionalFixedDepositList.isPresent());
    assertEquals(2, optionalFixedDepositList.get().getFixedDepositCount());
    List<String> fdNumbers =
        optionalFixedDepositList.get().getFixedDepositList().stream()
            .map(FixedDepositProto.FixedDeposit::getFdNumber)
            .sorted()
            .toList();
    assertEquals(2, fdNumbers.size());
    assertEquals("54678", fdNumbers.get(0));
    assertEquals("d43r23", fdNumbers.get(1));
  }

  @Test
  void testGetNetActiveFixedDepositPrincipalAmountForCurrency() {
    generateCadFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.CAD));
    generateInrFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.INR));

    OptionalDouble optionalDouble =
        bankFixedDepositsWarehouse.getNetActiveFixedDepositPrincipalAmountForCurrency(
            BankProto.CurrencyCode.CAD);
    assertTrue(optionalDouble.isPresent());
    assertEquals(9500, optionalDouble.getAsDouble(), DELTA_PRECISION);
  }

  @Test
  void testGetNetActiveFixedDepositExpectedAmountForCurrency() {
    generateCadFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.CAD));
    generateInrFixedDepositList()
        .getFixedDepositList()
        .forEach(fd -> bankFixedDepositsWarehouse.addFixedDeposit(fd, BankProto.CurrencyCode.INR));

    OptionalDouble optionalDouble =
        bankFixedDepositsWarehouse.getNetActiveFixedDepositExpectedAmountForCurrency(
            BankProto.CurrencyCode.CAD);
    assertTrue(optionalDouble.isPresent());
    assertEquals(11150, optionalDouble.getAsDouble(), DELTA_PRECISION);
  }

  private FixedDepositProto.FixedDepositList generateCadFixedDepositList() {
    return FixedDepositProto.FixedDepositList.newBuilder()
        .addFixedDeposit(
            FixedDepositProto.FixedDeposit.newBuilder()
                .setFdNumber("12345")
                .setDepositAmount(1000)
                .setExpectedAmount(1050)
                .setIsFdActive(false))
        .addFixedDeposit(
            FixedDepositProto.FixedDeposit.newBuilder()
                .setFdNumber("54678")
                .setDepositAmount(5000)
                .setExpectedAmount(6150)
                .setIsFdActive(true))
        .addFixedDeposit(
            FixedDepositProto.FixedDeposit.newBuilder()
                .setFdNumber("d43r23")
                .setDepositAmount(4500)
                .setExpectedAmount(5000)
                .setIsFdActive(true))
        .build();
  }

  private FixedDepositProto.FixedDepositList generateInrFixedDepositList() {
    return FixedDepositProto.FixedDepositList.newBuilder()
        .addFixedDeposit(
            FixedDepositProto.FixedDeposit.newBuilder()
                .setFdNumber("99999")
                .setDepositAmount(150000)
                .setExpectedAmount(200001)
                .setIsFdActive(true))
        .build();
  }
}
