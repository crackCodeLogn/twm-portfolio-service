package com.vv.personal.twm.portfolio.service.impl;

import com.google.common.collect.Sets;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.InvestmentDivWeight;
import com.vv.personal.twm.portfolio.model.market.InvestmentType;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.service.InvestmentDivWeightService;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import java.util.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2024-11-11
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InvestmentDivWeightServiceImpl implements InvestmentDivWeightService {

  private final MarketDataPythonEngineFeign marketDataPythonEngineFeign;
  private final MarketDataCrdbServiceFeign
      marketDataCrdbServiceFeign; // todo - replace with the data warehouse table?
  private final TickerDataWarehouseService tickerDataWarehouseService;

  @Override
  public Optional<InvestmentDivWeight> calcInvestmentBasedOnDivWeight(
      List<String> highValueTickerList,
      List<String> highDivTickerList,
      Double income,
      Double incomeSplitPercentage,
      Double amountToConsiderForDistributionTfsa) {
    final double incomeToConsider = income * incomeSplitPercentage / 100.0;
    Double amountToConsiderForDistributionNr =
        incomeToConsider - amountToConsiderForDistributionTfsa;
    if (amountToConsiderForDistributionNr < 0) {
      log.info(
          "Incorrect amount obtained for NR: {}. Income supplied: {}, income split percentage: {}, amount "
              + "for TFSA: {}",
          amountToConsiderForDistributionNr,
          income,
          incomeSplitPercentage,
          amountToConsiderForDistributionTfsa);
      return Optional.empty();
    }

    Map<String, Double> tickerDividendMap = new HashMap<>();
    populateTickerDividendMap(highValueTickerList, tickerDividendMap);
    populateTickerDividendMap(highDivTickerList, tickerDividendMap);

    InvestmentDivWeight investmentDivWeight =
        new InvestmentDivWeight(
            amountToConsiderForDistributionTfsa, amountToConsiderForDistributionNr);

    List<InvestmentDivWeight.Allocation> highDivWeights =
        computeDivWeights(highDivTickerList, tickerDividendMap);
    List<InvestmentDivWeight.Allocation> highValueWeights =
        computeDivWeights(highValueTickerList, tickerDividendMap);

    List<Double> highValueBasketGain =
        calculateBasketGain(highValueTickerList, highValueWeights, 25);
    double averageHighValueBasketGain = calcAverage(highValueBasketGain);
    List<Double> highDivBasketGain = calculateBasketGain(highDivTickerList, highDivWeights, 25);
    double averageHighDivBasketGain = calcAverage(highDivBasketGain);

    log.info("Avg high val basket gain: {}", averageHighValueBasketGain);
    log.info("Avg high div basket gain: {}", averageHighDivBasketGain);

    double averageBasketGain = averageHighDivBasketGain + averageHighValueBasketGain;
    double highDivWeight = averageHighDivBasketGain / averageBasketGain;

    double normalizedHighDivWeight = Math.min(0.60, Math.max(0.45, highDivWeight));
    double normalizedHighValWeight = 1 - normalizedHighDivWeight;

    investmentDivWeight.setNormalizedHighValueWeight(normalizedHighValWeight);
    investmentDivWeight.setHighValueWeight(1 - highDivWeight);
    investmentDivWeight.setNormalizedHighDivWeight(normalizedHighDivWeight);
    investmentDivWeight.setHighDivWeight(highDivWeight);

    // tfsa compute
    double tfsaAmountTotal = investmentDivWeight.getAmountToConsiderForDistributionTfsa();
    // high val
    double tfsaHighValAmount = tfsaAmountTotal * normalizedHighValWeight;
    compute(
        tfsaHighValAmount,
        highValueWeights,
        investmentDivWeight,
        MarketDataProto.AccountType.TFSA,
        InvestmentType.HIGH_VALUE);
    // high div
    double tfsaHighDivAmount = tfsaAmountTotal * normalizedHighDivWeight;
    compute(
        tfsaHighDivAmount,
        highDivWeights,
        investmentDivWeight,
        MarketDataProto.AccountType.TFSA,
        InvestmentType.HIGH_DIV);

    // nr compute
    double nrAmountTotal = investmentDivWeight.getAmountToConsiderForDistributionNr();
    // high val
    double nrHighValAmount = nrAmountTotal * normalizedHighValWeight;
    compute(
        nrHighValAmount,
        highValueWeights,
        investmentDivWeight,
        MarketDataProto.AccountType.NR,
        InvestmentType.HIGH_VALUE);
    // high div
    double nrHighDivAmount = nrAmountTotal * normalizedHighDivWeight;
    compute(
        nrHighDivAmount,
        highDivWeights,
        investmentDivWeight,
        MarketDataProto.AccountType.NR,
        InvestmentType.HIGH_DIV);

    return Optional.of(investmentDivWeight);
  }

  public void compute(
      Double amount,
      List<InvestmentDivWeight.Allocation> allocations,
      InvestmentDivWeight investmentDivWeight,
      MarketDataProto.AccountType accountType,
      InvestmentType investmentType) {

    allocations.forEach(
        allocation -> allocation.setAllocation(amount * allocation.getAllocationPercent()));
    allocations.sort(
        Comparator.comparingDouble(InvestmentDivWeight.Allocation::getAllocationPercent));

    if (investmentType == InvestmentType.HIGH_DIV) {
      allocations.forEach(
          allocation ->
              investmentDivWeight.addHighDivAllocation(
                  accountType,
                  allocation.getTicker(),
                  allocation.getAllocation(),
                  allocation.getAllocationPercent()));
    } else if (investmentType == InvestmentType.HIGH_VALUE) {
      allocations.forEach(
          allocation ->
              investmentDivWeight.addHighValueAllocation(
                  accountType,
                  allocation.getTicker(),
                  allocation.getAllocation(),
                  allocation.getAllocationPercent()));
    }
  }

  public List<Double> calculateBasketGain(
      List<String> tickerList, List<InvestmentDivWeight.Allocation> allocations, int deltaDays) {
    if (deltaDays <= 1) {
      log.error("Cannot process basket gain for delta days: {}", deltaDays);
      return new ArrayList<>();
    }

    Map<String, Double> tickerWeightMap = getTickerWeightMap(allocations);
    List<Double> averageGains = new ArrayList<>();

    for (String ticker : tickerList) {
      Optional<MarketDataProto.Portfolio> optionalPortfolio =
          marketDataCrdbServiceFeign.getLimitedDataByTicker(ticker, deltaDays);
      if (optionalPortfolio.isEmpty()) {
        log.warn(
            "No market data available for basket analysis for {}. Attempting downloading of missing ticker data",
            ticker);
        tickerDataWarehouseService.loadAnalysisDataForInstruments(Sets.newHashSet(ticker), false);
        optionalPortfolio = marketDataCrdbServiceFeign.getLimitedDataByTicker(ticker, deltaDays);

        if (optionalPortfolio.isEmpty()) {
          log.warn(
              "Still no market data available for basket analysis for {}. Skipping for now",
              ticker);
        }
        continue;
      }
      MarketDataProto.Portfolio portfolio = optionalPortfolio.get();
      List<Double> values =
          portfolio.getInstruments(0).getTicker().getDataList().stream()
              .map(MarketDataProto.Value::getPrice)
              .toList();
      int index = 0;
      double price2 = values.get(index);
      double gains = 0.0;
      int num = 0;
      for (index = 1; index < values.size(); index++, num++) {
        double gain = calcGain(values.get(index), price2);
        gain *= tickerWeightMap.get(ticker); // weight multiplier
        gains += gain;
      }
      gains /= num;
      gains *= 100.0;
      averageGains.add(gains);
    }

    return averageGains;
  }

  private Map<String, Double> getTickerWeightMap(List<InvestmentDivWeight.Allocation> allocations) {
    Map<String, Double> tickerWeightMap = new HashMap<>();
    allocations.forEach(
        allocation ->
            tickerWeightMap.put(allocation.getTicker(), allocation.getAllocationPercent()));
    return tickerWeightMap;
  }

  double calcGain(Double price1, double price2) {
    return (price2 - price1) / price1;
  }

  double calcAverage(List<Double> list) {
    double avg = 0.0;
    for (Double val : list) avg += val;
    return avg / list.size();
  }

  List<InvestmentDivWeight.Allocation> computeDivWeights(
      List<String> tickerList, Map<String, Double> tickerDividendMap) {

    List<InvestmentDivWeight.Allocation> allocationList = new ArrayList<>();
    double totalDiv = 0.0;
    boolean anyNullDiv = false;
    for (String ticker : tickerList) {
      double div = tickerDividendMap.getOrDefault(ticker, 0.0);
      if (Double.compare(div, 0.0) == 0) {
        log.warn(
            "Switching off weight calculation and defaulting to equal weights as did not find div weight for {}",
            ticker);
        anyNullDiv = true;
      }
      totalDiv += div;
    }

    for (String ticker : tickerList) {
      double weight =
          !anyNullDiv
              ? tickerDividendMap.getOrDefault(ticker, 0.0) / totalDiv
              : 1.0 / tickerList.size();
      allocationList.add(
          InvestmentDivWeight.Allocation.builder()
              .allocationPercent(weight)
              .ticker(ticker)
              .build());
    }
    return allocationList;
  }

  private void populateTickerDividendMap(
      List<String> tickerList, Map<String, Double> tickerDividendMap) {
    tickerList.forEach(
        ticker -> {
          log.info("Obtaining dividend yield for {}", ticker);
          String dividendStr =
              marketDataPythonEngineFeign.getTickerDividend(ticker); // without country code
          double dividend = 0.0;
          try {
            dividend = Double.parseDouble(dividendStr);
          } catch (NumberFormatException e) {
            log.error("Error while obtaining dividend for {}. ", ticker, e);
          }
          tickerDividendMap.put(ticker, dividend);
        });
  }
}
