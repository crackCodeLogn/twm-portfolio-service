package com.vv.personal.twm.portfolio.service.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.protobuf.ProtocolStringList;
import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.NetWorthBreakDownKey;
import com.vv.personal.twm.portfolio.service.CentralDataPointService;
import com.vv.personal.twm.portfolio.service.CompleteBankDataService;
import com.vv.personal.twm.portfolio.service.CompleteMarketDataService;
import com.vv.personal.twm.portfolio.service.ComputeMarketStatisticsService;
import com.vv.personal.twm.portfolio.service.InstrumentMetaDataService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2025-02-09
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class CentralDataPointServiceImpl implements CentralDataPointService {

  private final CompleteBankDataService completeBankDataService;
  private final CompleteMarketDataService completeMarketDataService;
  private final InstrumentMetaDataService instrumentMetaDataService;
  private final ComputeMarketStatisticsService computeMarketStatisticsService;

  @Override
  public Map<String, Double> getLatestTotalNetWorthBreakDown(BankProto.CurrencyCode ccy) {
    double marketNetWorth = getLatestMarketNetWorth().orElse(0.0);
    double gicNetWorth = 0.0;
    try {
      gicNetWorth = getGicValuations(ccy).lastEntry().getValue();
    } catch (Exception e) {
      log.warn("error while calculating gic net worth", e);
    }
    double bankNetWorth = getLatestBankNetWorth(ccy).orElse(0.0);
    double otherWorth = getLatestOtherNetWorth(ccy).orElse(0.0);
    double netWorth = marketNetWorth + gicNetWorth + bankNetWorth + otherWorth;
    return ImmutableMap.<String, Double>builder()
        .put(NetWorthBreakDownKey.NET_WORTH.getKey(), netWorth)
        .put(NetWorthBreakDownKey.MARKET.getKey(), marketNetWorth)
        .put(NetWorthBreakDownKey.GIC.getKey(), gicNetWorth)
        .put(NetWorthBreakDownKey.BANK.getKey(), bankNetWorth)
        .put(NetWorthBreakDownKey.OTHER.getKey(), otherWorth)
        .build();
  }

  @Override
  public OptionalDouble getLatestTotalNetWorth(BankProto.CurrencyCode ccy) {
    return OptionalDouble.of(
        getLatestTotalNetWorthBreakDown(ccy).get(NetWorthBreakDownKey.NET_WORTH.getKey()));
  }

  @Override
  public OptionalDouble getLatestBankNetWorth(BankProto.CurrencyCode ccy) {
    return completeBankDataService.getNetBankAccountBalanceForCurrency(ccy);
  }

  @Override
  public OptionalDouble getLatestOtherNetWorth(BankProto.CurrencyCode ccy) {
    return completeBankDataService.getOtherNetBalanceForCurrency(ccy);
  }

  @Override
  public OptionalDouble getLatestMarketNetWorth() {
    return OptionalDouble.of(
        completeMarketDataService.getLatestCombinedCumulativePnL()
            + completeMarketDataService.getLatestTotalInvestmentAmount());
  }

  @Override
  public FixedDepositProto.FixedDepositList getGicExpiries(BankProto.CurrencyCode currency) {
    return completeBankDataService.getGicExpiries(currency);
  }

  @Override
  public TreeMap<Integer, Double> getGicValuations(BankProto.CurrencyCode currency) {
    return completeBankDataService.getCumulativeDateAmountGicMap();
  }

  @Override
  public List<String> getMarketValuations(boolean includeDividends) {
    return completeMarketDataService.getMarketValuations(includeDividends);
  }

  @Override
  public Map<String, String> getMarketValuation(
      String imnt, MarketDataProto.AccountType accountType) {
    return completeMarketDataService.getMarketValuation(imnt, accountType);
  }

  @Override
  public TreeMap<Integer, Double> getMarketValuationsForPlot(
      MarketDataProto.AccountType accountType) {
    TreeMap<Integer, Double> marketValuations = new TreeMap<>();
    TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> combinedDatePnLCumulativeMap =
        completeMarketDataService.getCombinedDatePnLCumulativeMap();
    combinedDatePnLCumulativeMap.forEach(
        (date, accountValueMap) -> marketValuations.put(date, accountValueMap.get(accountType)));
    return marketValuations;
  }

  @Override
  public TreeMap<Integer, Double> getMarketValuationsForPlot() {
    TreeMap<Integer, Double> marketValuations = new TreeMap<>();
    TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> combinedDatePnLCumulativeMap =
        completeMarketDataService.getCombinedDatePnLCumulativeMap();
    combinedDatePnLCumulativeMap.forEach(
        (date, accountValueMap) -> {
          double value = 0.0;
          for (Double accountValue : accountValueMap.values()) value += accountValue;
          marketValuations.put(date, value);
        });
    return marketValuations;
  }

  @Override
  public Map<String, Double> getCumulativeImntDividendValuations(
      MarketDataProto.AccountType accountType) {
    Map<String, Double> cumulativeImntDividendValuations = new HashMap<>();

    Map<String, Map<MarketDataProto.AccountType, Double>> cumulativeImntAccountTypeDividendMap =
        completeMarketDataService.getCumulativeImntAccountTypeDividendMap();
    cumulativeImntAccountTypeDividendMap.forEach(
        (imnt, accountValueMap) -> {
          if (accountValueMap != null && accountValueMap.get(accountType) != null) {
            cumulativeImntDividendValuations.put(imnt, accountValueMap.get(accountType));
          }
        });
    return cumulativeImntDividendValuations;
  }

  @Override
  public Map<String, Double> getSectorLevelAggrDataMap(MarketDataProto.AccountType accountType) {
    return completeMarketDataService.getSectorLevelAggrDataMap(accountType);
  }

  @Override
  public Map<String, String> getSectorLevelImntAggrDataMap(
      MarketDataProto.AccountType accountType) {
    return completeMarketDataService.getSectorLevelImntAggrDataMap(accountType);
  }

  @Override
  public Map<String, String> getBestAndWorstPerformers(
      MarketDataProto.AccountType accountType, int n, boolean includeDividends) {
    return completeMarketDataService.getBestAndWorstPerformers(accountType, n, includeDividends);
  }

  @Override
  public DataPacketProto.DataPacket getDividendYieldAndSectorForAllImnts() {
    Map<String, Double> imntDividendMap =
        instrumentMetaDataService.getAllImntsDividendYieldPercentage();
    Map<String, String> imntSectorMap = completeMarketDataService.getAllImntsSector();

    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringStringMap(imntSectorMap)
        .putAllStringDoubleMap(imntDividendMap)
        .build();
  }

  @Override
  public Map<String, Double> getNetMarketValuations(
      Optional<MarketDataProto.AccountType> optionalAccountType, boolean includeDividends) {
    return completeMarketDataService.getNetMarketValuations(optionalAccountType, includeDividends);
  }

  @Override
  public OptionalInt forceDownloadMarketDataForDates(String imnt, String start, String end) {
    return OptionalInt.of(
        completeMarketDataService.forceDownloadMarketDataForDates(imnt, start, end));
  }

  @Override
  public Optional<Table<String, String, Double>> getCorrelationMatrix() {
    return completeMarketDataService.getCorrelationMatrix();
  }

  @Override
  public Optional<Table<String, String, Double>> getCorrelationMatrix(
      ProtocolStringList targetImnts) {
    List<String> targetInstruments = targetImnts.stream().toList();
    return completeMarketDataService.getCorrelationMatrix(targetInstruments);
  }

  @Override
  public OptionalDouble getCorrelation(String imnt1, String imnt2) {
    return completeMarketDataService.getCorrelation(imnt1, imnt2);
  }

  @Override
  public Optional<Table<String, String, Double>> getCorrelationMatrix(
      MarketDataProto.AccountType accType) {
    return completeMarketDataService.getCorrelationMatrix(accType);
  }

  @Override
  public Optional<Table<String, String, Double>> getCorrelationMatrixForSectors() {
    return completeMarketDataService.getCorrelationMatrixForSectors();
  }

  @Override
  public MarketDataProto.Portfolio getEntireMetaData() {
    return instrumentMetaDataService.getEntireMetaData();
  }

  @Override
  public MarketDataProto.Instrument getInstrumentMetaData(String imnt) {
    return instrumentMetaDataService.getInstrumentMetaData(imnt);
  }

  @Override
  public String upsertInstrumentMetaData(String imnt, DataPacketProto.DataPacket dataPacket) {
    return instrumentMetaDataService.upsertInstrumentMetaData(imnt, dataPacket);
  }

  @Override
  public String deleteInstrumentMetaData(String imnt) {
    return instrumentMetaDataService.deleteInstrumentMetaData(imnt);
  }

  @Override
  public String deleteEntireMetaData() {
    return instrumentMetaDataService.deleteEntireMetaData();
  }

  @Override
  public String bulkAddEntireMetaData(DataPacketProto.DataPacket dataPacket) {
    return instrumentMetaDataService.bulkAddEntireMetaData(dataPacket);
  }

  @Override
  public String backupEntireMetaData() {
    return instrumentMetaDataService.backup();
  }

  @Override
  public String reloadMetaDataCache() {
    return instrumentMetaDataService.reloadMetaDataCache();
  }

  @Override
  public MarketDataProto.Portfolio getCorporateActionNews() {
    return instrumentMetaDataService.getCorporateActionNews();
  }

  @Override
  public DataPacketProto.DataPacket getHeadingAtAGlance() {
    return completeMarketDataService.getHeadingAtAGlance();
  }

  @Override
  public MarketDataProto.Portfolio invokePortfolioOptimizer(
      MarketDataProto.AccountType accountType,
      double targetBeta,
      double maxVol,
      double maxPe,
      double maxWeight,
      double minYield,
      double newCash,
      double forceCash,
      String objectiveMode,
      String ignoreImnts,
      String forceImnts,
      String imntsScope) {
    return completeMarketDataService.invokePortfolioOptimizer(
        accountType,
        targetBeta,
        maxVol,
        maxPe,
        maxWeight,
        minYield,
        newCash,
        forceCash,
        objectiveMode,
        ignoreImnts,
        forceImnts,
        imntsScope);
  }

  @Override
  public void testInfo() {
    completeMarketDataService.testInfo();
  }
}
