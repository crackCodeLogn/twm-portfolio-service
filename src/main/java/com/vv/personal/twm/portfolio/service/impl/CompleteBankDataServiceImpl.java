package com.vv.personal.twm.portfolio.service.impl;

import static com.vv.personal.twm.portfolio.util.SanitizerUtil.sanitizeDouble;

import com.google.common.collect.Sets;
import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.remote.feign.BankCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.CalcServiceFeign;
import com.vv.personal.twm.portfolio.service.CompleteBankDataService;
import com.vv.personal.twm.portfolio.warehouse.bank.BankAccountWarehouse;
import com.vv.personal.twm.portfolio.warehouse.bank.BankFixedDepositsWarehouse;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2024-12-26
 */
@Slf4j
@Getter
@Service
@RequiredArgsConstructor
public class CompleteBankDataServiceImpl implements CompleteBankDataService {

  private final BankAccountWarehouse bankAccountWarehouse;
  private final BankFixedDepositsWarehouse bankFixedDepositsWarehouse;
  private final BankCrdbServiceFeign bankCrdbServiceFeign;
  private final CalcServiceFeign calcServiceFeign;
  private final DateLocalDateCache dateLocalDateCache;

  private final Map<FixedDepositProto.AccountType, TreeMap<Integer, Double>>
      accountTypeDateAmountGicMap = new HashMap<>();
  private final TreeMap<Integer, Double> combinedDateAmountGicMap = new TreeMap<>();
  private final TreeMap<Integer, Double> cumulativeDateAmountGicMap = new TreeMap<>();

  @Override
  public void load() {
    log.info("Initiating complete bank data load");
    StopWatch stopWatch = StopWatch.createStarted();
    // get CAD based bank accounts only, for now
    FixedDepositProto.FilterBy filterByCcyField = FixedDepositProto.FilterBy.CCY;
    BankProto.CurrencyCode currencyCodeCad = BankProto.CurrencyCode.CAD;

    BankProto.BankAccounts cadBankAccounts =
        bankCrdbServiceFeign.getBankAccounts(filterByCcyField.name(), currencyCodeCad.name());
    if (cadBankAccounts != null) populateBankAccounts(cadBankAccounts);

    FixedDepositProto.FixedDepositList fixedDepositList =
        bankCrdbServiceFeign.getFixedDeposits(filterByCcyField.name(), currencyCodeCad.name());
    if (fixedDepositList != null) {
      populateFixedDeposits(fixedDepositList, currencyCodeCad);
      calcGicMaps();
    }

    stopWatch.stop();
    log.info("Complete bank data load finished in {}ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  @Override
  public void clear() {
    log.warn("Initiating complete bank data clear");
    bankAccountWarehouse.clear();
    bankFixedDepositsWarehouse.clear();

    accountTypeDateAmountGicMap.clear();
    combinedDateAmountGicMap.clear();
    cumulativeDateAmountGicMap.clear();
    log.info("Completed bank data clearing");
  }

  // todo - write test
  @Override
  public FixedDepositProto.FixedDepositList getGicExpiries(BankProto.CurrencyCode currencyCode) {
    if (currencyCode == BankProto.CurrencyCode.UNRECOGNIZED) {
      log.error("Unrecognized currency code: {}", currencyCode);
      return FixedDepositProto.FixedDepositList.newBuilder().build();
    }

    Optional<FixedDepositProto.FixedDepositList> activeFixedDeposits =
        bankFixedDepositsWarehouse.getActiveFixedDeposits(currencyCode);
    if (activeFixedDeposits.isEmpty()) {
      log.warn("No fixed deposits found for code: {}", currencyCode);
      return FixedDepositProto.FixedDepositList.newBuilder().build();
    }
    List<FixedDepositProto.FixedDeposit> gicList =
        new ArrayList<>(activeFixedDeposits.get().getFixedDepositList());
    gicList.sort(Comparator.comparing(FixedDepositProto.FixedDeposit::getEndDate));
    return FixedDepositProto.FixedDepositList.newBuilder().addAllFixedDeposit(gicList).build();
  }

  @Override
  public OptionalDouble getNetBankAccountBalanceForCurrency(BankProto.CurrencyCode currencyCode) {
    return bankAccountWarehouse.getNetBankAccountBalanceForCurrency(
        currencyCode,
        Sets.newHashSet(
            BankProto.BankAccountType.GIC,
            BankProto.BankAccountType.MKT,
            BankProto.BankAccountType.CASH_R),
        false); // get all bank accounts sans GIC and sans MKT type
  }

  @Override
  public OptionalDouble getOtherNetBalanceForCurrency(BankProto.CurrencyCode currencyCode) {
    return bankAccountWarehouse.getNetBankAccountBalanceForCurrency(
        currencyCode,
        Sets.newHashSet(BankProto.BankAccountType.CASH_R),
        true); // get all bank accounts with CASH_R type
  }

  // todo - write test
  void populateBankAccounts(BankProto.BankAccounts cadBankAccounts) {
    cadBankAccounts.getAccountsList().forEach(bankAccountWarehouse::addBankAccount);
  }

  // todo - write test
  void populateFixedDeposits(
      FixedDepositProto.FixedDepositList fixedDepositList, BankProto.CurrencyCode currencyCode) {
    fixedDepositList
        .getFixedDepositList()
        .forEach(deposit -> bankFixedDepositsWarehouse.addFixedDeposit(deposit, currencyCode));
  }

  // todo - write test - too complex, but needs to be done
  void calcGicMaps() {
    Optional<FixedDepositProto.FixedDepositList> allFixedDepositsOptional =
        bankFixedDepositsWarehouse.getAllFixedDeposits();
    if (allFixedDepositsOptional.isEmpty()) {
      log.warn("No fixed deposits found");
      return;
    }
    List<FixedDepositProto.FixedDeposit> allFixedDeposits =
        allFixedDepositsOptional.get().getFixedDepositList().stream()
            .filter(FixedDepositProto.FixedDeposit::getIsFdActive)
            .toList();
    Map<FixedDepositProto.AccountType, List<FixedDepositProto.FixedDeposit>> accountTypeFdListMap =
        new HashMap<>();
    allFixedDeposits.forEach(
        deposit -> {
          accountTypeFdListMap.computeIfAbsent(deposit.getAccountType(), k -> new ArrayList<>());
          accountTypeFdListMap.get(deposit.getAccountType()).add(deposit);
        });

    accountTypeDateAmountGicMap.clear();
    accountTypeFdListMap.forEach(
        (accountType, fixedDeposits) -> {
          FixedDepositProto.FixedDepositList deposits =
              FixedDepositProto.FixedDepositList.newBuilder()
                  .addAllFixedDeposit(fixedDeposits)
                  .build();
          accountTypeDateAmountGicMap.computeIfAbsent(accountType, k -> new TreeMap<>());

          // compute map of date and amount for the deposits
          DataPacketProto.DataPacket dataPacket = calcServiceFeign.getFixedDepositAmounts(deposits);
          accountTypeDateAmountGicMap.get(accountType).putAll(dataPacket.getIntDoubleMapMap());
        });

    // fill combined map
    accountTypeDateAmountGicMap
        .values()
        .forEach(
            map ->
                map.forEach(
                    (date, amt) ->
                        combinedDateAmountGicMap.compute(date, (k, v) -> sanitizeDouble(v) + amt)));

    // cumulative compute ahead
    List<TreeMap<Integer, Double>> allGicDailyMapList = new ArrayList<>();
    int universalStartDate = Integer.MAX_VALUE, universalEndDate = Integer.MIN_VALUE;

    for (FixedDepositProto.FixedDeposit deposit : allFixedDeposits) {
      DataPacketProto.DataPacket dataPacket = calcServiceFeign.getFixedDepositAmount(deposit);
      TreeMap<Integer, Double> gicDailyMap = new TreeMap<>();
      dataPacket
          .getIntDoubleMapMap()
          .forEach(
              (date, amount) -> {
                gicDailyMap.put(date, amount);
                if (!dateLocalDateCache.contains(date)) dateLocalDateCache.add(date);
              });
      universalStartDate = Math.min(universalStartDate, gicDailyMap.firstKey());
      universalEndDate = Math.max(universalEndDate, gicDailyMap.lastKey());

      gicDailyMap.put(0, 0.0);
      allGicDailyMapList.add(gicDailyMap);
    }
    LocalDate endDate = dateLocalDateCache.get(universalEndDate).get().plusDays(1);

    for (LocalDate date = dateLocalDateCache.get(universalStartDate).get();
        date.isBefore(endDate);
        date = date.plusDays(1)) {
      double dateCumulativeAmount = 0.0;
      for (TreeMap<Integer, Double> gicDailyMap : allGicDailyMapList) {
        dateCumulativeAmount +=
            gicDailyMap.floorEntry(dateLocalDateCache.get(date).getAsInt()).getValue();
      }
      cumulativeDateAmountGicMap.put(dateLocalDateCache.get(date).getAsInt(), dateCumulativeAmount);
    }
  }
}
