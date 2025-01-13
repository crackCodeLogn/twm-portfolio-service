package com.vv.personal.twm.portfolio.service;

import static com.vv.personal.twm.portfolio.util.SanitizerUtil.sanitizeDouble;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.remote.feign.CalcServiceFeign;
import com.vv.personal.twm.portfolio.warehouse.bank.BankAccountWarehouse;
import com.vv.personal.twm.portfolio.warehouse.bank.BankFixedDepositsWarehouse;
import java.time.LocalDate;
import java.util.*;
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
  private final CalcServiceFeign calcServiceFeign;
  private final DateLocalDateCache dateLocalDateCache;

  private final Map<FixedDepositProto.AccountType, TreeMap<Integer, Double>>
      accountTypeDateAmountGicMap = new HashMap<>();
  private final TreeMap<Integer, Double> combinedDateAmountGicMap = new TreeMap<>();
  private final TreeMap<Integer, Double> cumulativeDateAmountGicMap = new TreeMap<>();

  public void populateBankAccounts(BankProto.BankAccounts cadBankAccounts) {
    cadBankAccounts.getAccountsList().forEach(bankAccountWarehouse::addBankAccount);
  }

  public void populateFixedDeposits(
      FixedDepositProto.FixedDepositList fixedDepositList, BankProto.CurrencyCode currencyCode) {
    fixedDepositList
        .getFixedDepositList()
        .forEach(deposit -> bankFixedDepositsWarehouse.addFixedDeposit(deposit, currencyCode));
  }

  public void calcGicMaps() {
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
