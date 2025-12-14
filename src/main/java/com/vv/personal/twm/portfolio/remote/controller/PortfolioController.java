package com.vv.personal.twm.portfolio.remote.controller;

import com.google.common.collect.Table;
import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.InvestmentDivWeight;
import com.vv.personal.twm.portfolio.service.CentralDataPointService;
import com.vv.personal.twm.portfolio.service.InvestmentDivWeightService;
import com.vv.personal.twm.portfolio.service.ReloadService;
import com.vv.personal.twm.portfolio.util.DataConverterUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Vivek
 * @since 2023-11-23
 */
@Slf4j
@RestController("portfolio")
@Controller
@RequestMapping("/portfolio/")
@CrossOrigin(origins = "http://localhost:5173") // Allow React app
@RequiredArgsConstructor
public class PortfolioController {

  private final InvestmentDivWeightService investmentDivWeightService;
  private final CentralDataPointService centralDataPointService;
  private final ReloadService reloadService;

  /*
  @Lazy
  @Qualifier("ticker-dwh-s")
  private final TickerDataWarehouse soldTickerDataWarehouse;

  @Lazy
  @Qualifier("ticker-dwh-b")
  private final TickerDataWarehouse boughtTickerDataWarehouse;*/

  @GetMapping("/reload/v2k")
  public String reloadV2kData() {
    if (reloadService.reload()) return "OK";
    else return "ERROR";
  }

  @GetMapping("/manual/investment/div-weights")
  public String getInvestmentDivWeights(
      @RequestParam(defaultValue = "VFV.TO, VCE.TO, XEQT.TO, ZDB.TO") String highValImnts,
      @RequestParam(
              defaultValue =
                  "RY.TO, BNS.TO, CM.TO, ENB.TO, AFM.V, SU.TO, CNQ.TO, BEP-UN.TO, DBM.TO, FTS.TO")
          String highDivImnts,
      @RequestParam(defaultValue = "5000.0") Double income,
      @RequestParam(defaultValue = "20.0") Double incomeSplitPercentage,
      @RequestParam(defaultValue = "0.0") Double forceAbsoluteAmount,
      @RequestParam(defaultValue = "0.0") Double amountToConsiderForDistributionTfsa) {

    List<String> highValList = DataConverterUtil.split(highValImnts);
    List<String> highDivList = DataConverterUtil.split(highDivImnts);
    log.info(
        "getInvestmentDivWeights invoked with {} high val imnts, {} high div imnts, {} income, {} income split "
            + "percentage and {} amount for tfsa",
        highValList.size(),
        highDivList.size(),
        income,
        incomeSplitPercentage,
        amountToConsiderForDistributionTfsa);

    if (forceAbsoluteAmount > 0.0) {
      log.warn("Overriding income amount with the forced absolute amount!");
      income = forceAbsoluteAmount;
      incomeSplitPercentage = 100.0;
    }

    Optional<InvestmentDivWeight> investmentDivWeight =
        investmentDivWeightService.calcInvestmentBasedOnDivWeight(
            highValList,
            highDivList,
            income,
            incomeSplitPercentage,
            amountToConsiderForDistributionTfsa);
    if (investmentDivWeight.isEmpty()) {
      log.warn("Unable to compute investment based on div weight distribution!");
      return "Unable to compute investment based on div weight distribution!";
    }

    log.info("Found investment break down on div weight");
    log.debug(investmentDivWeight.get().toString());
    return investmentDivWeight.get().toString();
  }

  @GetMapping("/gic/expiries")
  public FixedDepositProto.FixedDepositList getGicExpiries(@RequestParam("ccy") String ccy) {
    log.info("getGicExpiries invoked");
    BankProto.CurrencyCode code = BankProto.CurrencyCode.valueOf(ccy);
    return centralDataPointService.getGicExpiries(code);
  }

  @GetMapping("/gic/valuations")
  public DataPacketProto.DataPacket getGicValuations(@RequestParam("ccy") String ccy) {
    log.info("getGicValuations invoked");
    BankProto.CurrencyCode code = BankProto.CurrencyCode.valueOf(ccy);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllIntDoubleMap(centralDataPointService.getGicValuations(code))
        .build();
  }

  @GetMapping("/market/valuation/account")
  public DataPacketProto.DataPacket getMarketImntAccountValuation(
      @RequestParam("imnt") String imnt, @RequestParam("accountType") String accountType) {
    log.info("getMarketAccountValuation {} invoked", accountType);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringStringMap(centralDataPointService.getMarketValuation(imnt, accountTypeEnum))
        .build();
  }

  @GetMapping("/market/valuations/dividends")
  public DataPacketProto.DataPacket getCumulativeImntDividendValuations(
      @RequestParam("accountType") String accountType) {
    log.info("getCumulativeImntDividendValuations {} invoked", accountType);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringDoubleMap(
            centralDataPointService.getCumulativeImntDividendValuations(accountTypeEnum))
        .build();
  }

  @GetMapping("/market/valuations")
  public DataPacketProto.DataPacket getMarketValuations(
      @RequestParam("divs") boolean includeDividends) {
    log.info(
        "getMarketValuations invoked for data aggr for imnt across account types with divs {}",
        includeDividends);
    return DataPacketProto.DataPacket.newBuilder()
        .addAllStrings(centralDataPointService.getMarketValuations(includeDividends))
        .build();
  }

  @GetMapping("/market/valuations/net")
  public DataPacketProto.DataPacket getNetMarketValuations(
      @RequestParam("divs") boolean includeDividends) {
    log.info(
        "getNetMarketValuations invoked for data aggr for imnt across account types with divs {}",
        includeDividends);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringDoubleMap(
            centralDataPointService.getNetMarketValuations(Optional.empty(), includeDividends))
        .build();
  }

  @GetMapping("/market/valuations/net/account")
  public DataPacketProto.DataPacket getNetMarketValuations(
      @RequestParam("accountType") String accountType,
      @RequestParam("divs") boolean includeDividends) {
    log.info(
        "getNetMarketValuations invoked for data aggr for imnt for account type {} with divs {}",
        accountType,
        includeDividends);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringDoubleMap(
            centralDataPointService.getNetMarketValuations(
                Optional.of(accountTypeEnum), includeDividends))
        .build();
  }

  @GetMapping("/market/valuations/plot")
  public DataPacketProto.DataPacket getMarketValuationsForPlot() {
    log.info("getMarketValuationsForPlot invoked");
    return DataPacketProto.DataPacket.newBuilder()
        .putAllIntDoubleMap(centralDataPointService.getMarketValuationsForPlot())
        .build();
  }

  @GetMapping("/market/valuations/plot/account")
  public DataPacketProto.DataPacket getMarketAccountValuations(
      @RequestParam("accountType") String accountType) {
    log.info("getMarketAccountValuations {} invoked", accountType);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllIntDoubleMap(centralDataPointService.getMarketValuationsForPlot(accountTypeEnum))
        .build();
  }

  @GetMapping("/market/valuations/sector/account")
  public DataPacketProto.DataPacket getMarketAccountSectorValuations(
      @RequestParam("accountType") String accountType) {
    log.info("getMarketAccountSectorValuations {} invoked", accountType);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringDoubleMap(centralDataPointService.getSectorLevelAggrDataMap(accountTypeEnum))
        .build();
  }

  @GetMapping("/market/valuations/sector-imnt/account")
  public DataPacketProto.DataPacket getMarketAccountSectorImntValuations(
      @RequestParam("accountType") String accountType) {
    log.info("getMarketAccountSectorImntValuations {} invoked", accountType);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringStringMap(
            centralDataPointService.getSectorLevelImntAggrDataMap(accountTypeEnum))
        .build();
  }

  @GetMapping("/market/valuations/best-worst/account")
  public DataPacketProto.DataPacket getMarketAccountBestAndWorstValuations(
      @RequestParam("accountType") String accountType,
      @RequestParam("n") int n,
      @RequestParam("divs") boolean includeDividends) {
    log.info("getMarketAccountBestAndWorstValuations {} invoked", accountType);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringStringMap(
            centralDataPointService.getBestAndWorstPerformers(accountTypeEnum, n, includeDividends))
        .build();
  }

  @GetMapping("/market/info/imnts/dividend-yield-sector")
  public DataPacketProto.DataPacket getDividendYieldAndSectorForAllImnts() {
    log.info("getDividendYieldAndSectorForAllImnts invoked");

    return centralDataPointService.getDividendYieldAndSectorForAllImnts();
  }

  @GetMapping("/net-worth")
  public DataPacketProto.DataPacket getNetWorth(@RequestParam("ccy") String ccy) {
    log.info("getNetWorth invoked");
    BankProto.CurrencyCode code = BankProto.CurrencyCode.valueOf(ccy);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringDoubleMap(centralDataPointService.getLatestTotalNetWorthBreakDown(code))
        .build();
  }

  @GetMapping("/correlation/matrix")
  public MarketDataProto.CorrelationMatrix getCorrelationMatrix() {
    log.info("getCorrelationMatrix invoked");
    Optional<Table<String, String, Double>> optionalCorrelationMatrix =
        centralDataPointService.getCorrelationMatrix();
    MarketDataProto.CorrelationMatrix correlationMatrix =
        DataConverterUtil.getCorrelationMatrix(optionalCorrelationMatrix);
    log.info(
        "Correlation matrix proto created with {} entries", correlationMatrix.getEntriesCount());
    return correlationMatrix;
  }

  @GetMapping("/correlation")
  public String getCorrelation(
      @RequestParam("imnt1") String imnt1, @RequestParam("imnt2") String imnt2) {
    log.info("getCorrelationMatrix invoked for {} x {}", imnt1, imnt2);
    OptionalDouble optionalCorrelation = centralDataPointService.getCorrelation(imnt1, imnt2);
    String correlation = "ERR";
    if (optionalCorrelation.isPresent()) {
      correlation = String.valueOf(optionalCorrelation.getAsDouble());
    }
    log.info("Correlation compute between {} x {} => {}", imnt1, imnt2, correlation);
    return correlation;
  }

  @GetMapping("/")
  public String get() {
    return "hi";
  }

  @GetMapping("/manual/market/force/download-dates")
  public Integer forceDownloadMarketDataForDates(
      @RequestParam("imnt") String imnt,
      @RequestParam("start") String startDate,
      @RequestParam("end") String endDate) {
    log.info("getMarketAccountValuations {}:{}->{} invoked", imnt, startDate, endDate);
    OptionalInt imntRecordsDownloaded =
        centralDataPointService.forceDownloadMarketDataForDates(imnt, startDate, endDate);
    log.info("Downloaded {} imnt records for {}", imntRecordsDownloaded, imnt);
    return imntRecordsDownloaded.isPresent() ? imntRecordsDownloaded.getAsInt() : 0;
  }

  private List<String> split(String data) {
    return Arrays.stream(StringUtils.split(data, ","))
        .map(String::trim)
        .collect(Collectors.toList());
  }
}
