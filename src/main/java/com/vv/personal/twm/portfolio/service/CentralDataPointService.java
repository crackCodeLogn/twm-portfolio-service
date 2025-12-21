package com.vv.personal.twm.portfolio.service;

import com.google.common.collect.Table;
import com.google.protobuf.ProtocolStringList;
import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * @author Vivek
 * @since 2025-02-09
 */
public interface CentralDataPointService {

  /** Return the latest total net worth => market + bank. Current working currency => CAD */
  Map<String, Double> getLatestTotalNetWorthBreakDown(BankProto.CurrencyCode ccy);

  /** Return the latest total net worth => market + bank. Current working currency => CAD */
  OptionalDouble getLatestTotalNetWorth(BankProto.CurrencyCode ccy);

  /** Return the latest bank net worth for input currency */
  OptionalDouble getLatestBankNetWorth(BankProto.CurrencyCode ccy);

  /** Return the latest net other worth for input currency */
  OptionalDouble getLatestOtherNetWorth(BankProto.CurrencyCode ccy);

  /** Return the latest combined market net worth. Working currency => CAD */
  OptionalDouble getLatestMarketNetWorth();

  /** Return gic list in order of upcoming expiries for @param currency */
  FixedDepositProto.FixedDepositList getGicExpiries(BankProto.CurrencyCode currency);

  Map<Integer, Double> getGicValuations(BankProto.CurrencyCode currency);

  List<String> getMarketValuations(boolean includeDividends);

  Map<String, String> getMarketValuation(String imnt, MarketDataProto.AccountType accountType);

  Map<Integer, Double> getMarketValuationsForPlot(MarketDataProto.AccountType accountType);

  Map<Integer, Double> getMarketValuationsForPlot();

  Map<String, Double> getCumulativeImntDividendValuations(MarketDataProto.AccountType accountType);

  Map<String, Double> getSectorLevelAggrDataMap(MarketDataProto.AccountType accountType);

  Map<String, String> getSectorLevelImntAggrDataMap(MarketDataProto.AccountType accountType);

  Map<String, String> getBestAndWorstPerformers(
      MarketDataProto.AccountType accountType, int n, boolean includeDividends);

  DataPacketProto.DataPacket getDividendYieldAndSectorForAllImnts();

  Map<String, Double> getNetMarketValuations(
      Optional<MarketDataProto.AccountType> optionalAccountType, boolean includeDividends);

  OptionalInt forceDownloadMarketDataForDates(String imnt, String start, String end);

  Optional<Table<String, String, Double>> getCorrelationMatrix();

  Optional<Table<String, String, Double>> getCorrelationMatrix(ProtocolStringList targetImnts);

  OptionalDouble getCorrelation(String imnt1, String imnt2);

  Optional<Table<String, String, Double>> getCorrelationMatrix(MarketDataProto.AccountType accType);

  Optional<Table<String, String, Double>> getCorrelationMatrixForSectors();
}
