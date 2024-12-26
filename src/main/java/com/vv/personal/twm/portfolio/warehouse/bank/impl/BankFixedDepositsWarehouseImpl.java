package com.vv.personal.twm.portfolio.warehouse.bank.impl;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.portfolio.warehouse.bank.BankFixedDepositsWarehouse;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author Vivek
 * @since 2024-12-26
 */
@Slf4j
@Component
public class BankFixedDepositsWarehouseImpl implements BankFixedDepositsWarehouse {

  private final Map<String, FixedDepositProto.FixedDeposit> fixedDepositsMap;
  private final Map<BankProto.CurrencyCode, Set<String>> ccyFixedDepositIdsMap;

  public BankFixedDepositsWarehouseImpl() {
    this.fixedDepositsMap = new ConcurrentHashMap<>();
    this.ccyFixedDepositIdsMap = new ConcurrentHashMap<>();
  }

  @Override
  public void addFixedDeposit(
      FixedDepositProto.FixedDeposit fixedDeposit, BankProto.CurrencyCode currency) {
    String id = fixedDeposit.getFdNumber();
    if (!fixedDepositsMap.containsKey(id)) {
      fixedDepositsMap.put(id, fixedDeposit);
      ccyFixedDepositIdsMap.computeIfAbsent(currency, k -> new HashSet<>()).add(id);
    } else {
      log.warn("Duplicate fixed deposit with id {}", id);
    }
  }

  @Override
  public Optional<FixedDepositProto.FixedDeposit> getFixedDeposit(String id) {
    return fixedDepositsMap.containsKey(id)
        ? Optional.of(fixedDepositsMap.get(id))
        : Optional.empty();
  }

  @Override
  public Optional<FixedDepositProto.FixedDepositList> getFixedDepositsByCurrency(
      BankProto.CurrencyCode currency) {
    return ccyFixedDepositIdsMap.containsKey(currency)
        ? Optional.of(
            FixedDepositProto.FixedDepositList.newBuilder()
                .addAllFixedDeposit(
                    ccyFixedDepositIdsMap.get(currency).stream()
                        .map(fixedDepositsMap::get)
                        .toList())
                .build())
        : Optional.empty();
  }

  @Override
  public Optional<FixedDepositProto.FixedDepositList> getAllFixedDeposits() {
    return Optional.of(
        FixedDepositProto.FixedDepositList.newBuilder()
            .addAllFixedDeposit(fixedDepositsMap.values())
            .build());
  }

  @Override
  public Optional<FixedDepositProto.FixedDepositList> getActiveFixedDeposits(
      BankProto.CurrencyCode currency) {
    Optional<FixedDepositProto.FixedDepositList> fixedDeposits =
        getFixedDepositsByCurrency(currency);
    return fixedDeposits.map(
        fixedDepositList ->
            FixedDepositProto.FixedDepositList.newBuilder()
                .addAllFixedDeposit(
                    fixedDepositList.getFixedDepositList().stream()
                        .filter(FixedDepositProto.FixedDeposit::getIsFdActive)
                        .toList())
                .build());
  }

  @Override
  public OptionalDouble getNetActiveFixedDepositPrincipalAmountForCurrency(
      BankProto.CurrencyCode currencyCode) {
    return getActiveFixedDeposits(currencyCode)
        .map(
            fixedDepositList ->
                OptionalDouble.of(
                    fixedDepositList.getFixedDepositList().stream()
                        .mapToDouble(FixedDepositProto.FixedDeposit::getDepositAmount)
                        .sum()))
        .orElseGet(OptionalDouble::empty);
  }

  @Override
  public OptionalDouble getNetActiveFixedDepositExpectedAmountForCurrency(
      BankProto.CurrencyCode currencyCode) {
    return getActiveFixedDeposits(currencyCode)
        .map(
            fixedDepositList ->
                OptionalDouble.of(
                    fixedDepositList.getFixedDepositList().stream()
                        .mapToDouble(FixedDepositProto.FixedDeposit::getExpectedAmount)
                        .sum()))
        .orElseGet(OptionalDouble::empty);
  }

  @Override
  public void clear() {
    this.fixedDepositsMap.clear();
    this.ccyFixedDepositIdsMap.clear();
  }

  Map<String, FixedDepositProto.FixedDeposit> getFixedDepositsMap() {
    return fixedDepositsMap;
  }

  Map<BankProto.CurrencyCode, Set<String>> getCcyFixedDepositIdsMap() {
    return ccyFixedDepositIdsMap;
  }
}
