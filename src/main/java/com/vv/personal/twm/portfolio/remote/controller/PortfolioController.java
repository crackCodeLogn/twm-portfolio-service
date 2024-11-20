package com.vv.personal.twm.portfolio.remote.controller;

import com.vv.personal.twm.portfolio.model.market.InvestmentDivWeight;
import com.vv.personal.twm.portfolio.service.InvestmentDivWeightService;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Controller;
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
@RequiredArgsConstructor
public class PortfolioController {

  private final InvestmentDivWeightService investmentDivWeightService;

  /*
  @Lazy
  @Qualifier("ticker-dwh-s")
  private final TickerDataWarehouse soldTickerDataWarehouse;

  @Lazy
  @Qualifier("ticker-dwh-b")
  private final TickerDataWarehouse boughtTickerDataWarehouse;*/

  @GetMapping("/manual/investment/div-weights")
  public String getInvestmentDivWeights(
      @RequestParam(defaultValue = "VFV.TO, VDY.TO, VCE.TO, CCO.TO") String highValImnts,
      @RequestParam(
              defaultValue =
                  "RY.TO, BNS.TO, CM.TO, ENB.TO, BCE.TO, TRP.TO, SU.TO, CNQ.TO, CP.TO, PZA.TO, DBM.TO, FTS.TO")
          String highDivImnts,
      @RequestParam(defaultValue = "5000.0") Double income,
      @RequestParam(defaultValue = "20.0") Double incomeSplitPercentage,
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
