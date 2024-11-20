package com.vv.personal.twm.portfolio.model.market;

import com.google.common.collect.Lists;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Vivek
 * @since 2024-11-11
 */
@Setter
@Getter
public class InvestmentDivWeight {

  private final Map<MarketDataProto.AccountType, List<Allocation>> highValueTickerMap;
  private final Map<MarketDataProto.AccountType, List<Allocation>> highDivTickerMap;
  private final Double amountToConsiderForDistributionTfsa;
  private final Double amountToConsiderForDistributionNr;
  private Double normalizedHighDivWeight;
  private Double highDivWeight;
  private Double normalizedHighValueWeight;
  private Double highValueWeight;

  public InvestmentDivWeight(
      Double amountToConsiderForDistributionTfsa, Double amountToConsiderForDistributionNr) {
    this.amountToConsiderForDistributionTfsa = amountToConsiderForDistributionTfsa;
    this.amountToConsiderForDistributionNr = amountToConsiderForDistributionNr;
    this.highValueTickerMap = new HashMap<>();
    this.highDivTickerMap = new HashMap<>();

    Arrays.stream(MarketDataProto.AccountType.values())
        .forEach(type -> highValueTickerMap.put(type, Lists.newArrayList()));
    Arrays.stream(MarketDataProto.AccountType.values())
        .forEach(type -> highDivTickerMap.put(type, Lists.newArrayList()));
    highValueTickerMap.remove(MarketDataProto.AccountType.UNRECOGNIZED);
    highDivTickerMap.remove(MarketDataProto.AccountType.UNRECOGNIZED);
  }

  public void addHighValueAllocation(
      MarketDataProto.AccountType accountType,
      String ticker,
      Double allocation,
      Double allocationPercent) {
    List<Allocation> allocations = highValueTickerMap.get(accountType);
    allocations.add(generateAllocation(ticker, allocation, allocationPercent));
  }

  public void addHighDivAllocation(
      MarketDataProto.AccountType accountType,
      String ticker,
      Double allocation,
      Double allocationPercent) {
    List<Allocation> allocations = highDivTickerMap.get(accountType);
    allocations.add(generateAllocation(ticker, allocation, allocationPercent));
  }

  @Override
  public String toString() {
    StringBuilder data = new StringBuilder(LocalDate.now().toString());

    data.append("\n-----------------------------\n");
    data.append(
        String.format(
            "Total amount investing in TFSA: $%.2f\n\n", amountToConsiderForDistributionTfsa));
    data.append("*********************\n");
    double highValueTfsaAllocation =
        getAmountFromAllocation(highValueTickerMap.get(MarketDataProto.AccountType.TFSA));
    data.append(
        String.format("For TFSA-VAL, money to be allocated as: %.2f\n", highValueTfsaAllocation));
    int allocationId = 1;
    double left = highValueTfsaAllocation;
    for (Allocation allocation : highValueTickerMap.get(MarketDataProto.AccountType.TFSA)) {
      left -= allocation.getAllocation();
      data.append(getAllocationString(allocationId, allocation, left));
      allocationId++;
    }
    data.append("\n*********************\n");

    double highDivTfsaAllocation =
        getAmountFromAllocation(highDivTickerMap.get(MarketDataProto.AccountType.TFSA));
    data.append(
        String.format("For TFSA-DIV, money to be allocated as: %.2f\n", highDivTfsaAllocation));
    allocationId = 1;
    left = highDivTfsaAllocation;
    for (Allocation allocation : highDivTickerMap.get(MarketDataProto.AccountType.TFSA)) {
      left -= allocation.getAllocation();
      data.append(getAllocationString(allocationId, allocation, left));
      allocationId++;
    }

    data.append("\n-----------------------------\n");
    data.append(
        String.format(
            "Total amount investing in Non-registered: $%.2f\n\n",
            amountToConsiderForDistributionNr));
    data.append("*********************\n");
    double highValueNrAllocation =
        getAmountFromAllocation(highValueTickerMap.get(MarketDataProto.AccountType.NR));
    data.append(
        String.format("For NR-VAL, money to be allocated as: %.2f\n", highValueNrAllocation));
    allocationId = 1;
    left = highValueNrAllocation;
    for (Allocation allocation : highValueTickerMap.get(MarketDataProto.AccountType.NR)) {
      left -= allocation.getAllocation();
      data.append(getAllocationString(allocationId, allocation, left));
      allocationId++;
    }
    data.append("\n*********************\n");

    double highDivNrAllocation =
        getAmountFromAllocation(highDivTickerMap.get(MarketDataProto.AccountType.NR));
    data.append(String.format("For NR-DIV, money to be allocated as: %.2f\n", highDivNrAllocation));
    allocationId = 1;
    left = highDivNrAllocation;
    for (Allocation allocation : highDivTickerMap.get(MarketDataProto.AccountType.NR)) {
      left -= allocation.getAllocation();
      data.append(getAllocationString(allocationId, allocation, left));
      allocationId++;
    }
    data.append("\n-----------------------------");

    return data.toString();
  }

  private String getAllocationString(int allocationId, Allocation allocation, double left) {
    return String.format(
        "%d. %s: $%.2f -> %.2f%%, left: $%.2f\n",
        allocationId,
        allocation.getTicker(),
        allocation.getAllocation(),
        allocation.getAllocationPercent() * 100.0,
        left);
  }

  private Double getAmountFromAllocation(List<Allocation> allocations) {
    double sum = 0;
    for (int i = 0; i < allocations.size(); i++) sum += allocations.get(i).getAllocation();
    return sum;
  }

  private Allocation generateAllocation(
      String ticker, Double allocation, Double allocationPercent) {
    return Allocation.builder()
        .ticker(ticker)
        .allocation(allocation)
        .allocationPercent(allocationPercent)
        .build();
  }

  @Getter
  @Setter
  @Builder
  @ToString
  public static final class Allocation {
    private String ticker;
    private Double allocation;
    private Double allocationPercent;
  }
}
