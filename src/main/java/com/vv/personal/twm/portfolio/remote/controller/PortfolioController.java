package com.vv.personal.twm.portfolio.remote.controller;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.InvestmentDivWeight;
import com.vv.personal.twm.portfolio.service.CentralDataPointService;
import com.vv.personal.twm.portfolio.service.InvestmentDivWeightService;
import com.vv.personal.twm.portfolio.service.ReloadService;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

    List<String> highValList = split(highValImnts);
    List<String> highDivList = split(highDivImnts);
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

  @GetMapping("/market/valuations/account")
  public DataPacketProto.DataPacket getMarketAccountValuations(
      @RequestParam("accountType") String accountType) {
    log.info("getMarketAccountValuations {} invoked", accountType);
    MarketDataProto.AccountType accountTypeEnum = MarketDataProto.AccountType.valueOf(accountType);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllIntDoubleMap(centralDataPointService.getMarketValuations(accountTypeEnum))
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
  public DataPacketProto.DataPacket getMarketValuations() {
    log.info("getMarketValuations invoked");
    return DataPacketProto.DataPacket.newBuilder()
        .putAllIntDoubleMap(centralDataPointService.getMarketValuations())
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

  @GetMapping("/net-worth")
  public DataPacketProto.DataPacket getNetWorth(@RequestParam("ccy") String ccy) {
    log.info("getNetWorth invoked");
    BankProto.CurrencyCode code = BankProto.CurrencyCode.valueOf(ccy);
    return DataPacketProto.DataPacket.newBuilder()
        .putAllStringDoubleMap(centralDataPointService.getLatestTotalNetWorthBreakDown(code))
        .build();
  }

  @GetMapping("/")
  public String get() {
    return "hi";
  }

  @GetMapping("/data")
  public void getPortfolio() {
    log.info("Firing get portfolio request");
    // soldTickerDataWarehouse.generateData();
    log.info("get portf request completed");
  }

  private List<String> split(String data) {
    return Arrays.stream(StringUtils.split(data, ","))
        .map(String::trim)
        .collect(Collectors.toList());
  }
}
