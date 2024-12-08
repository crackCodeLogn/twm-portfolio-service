package com.vv.personal.twm.portfolio.service;

import static com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto.FilterBy.BANK;

import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.ping.config.PingConfig;
import com.vv.personal.twm.portfolio.model.market.CompleteMarketData;
import com.vv.personal.twm.portfolio.remote.feign.BankCrdbServiceFeign;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2024-08-11
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StartupService {

  private final PingConfig pingConfig;
  private final BankCrdbServiceFeign crdbServiceFeign;
  private final TickerDataWarehouseService tickerDataWarehouseService;
  private final CompleteMarketData completeMarketData;
  private final InvestmentDivWeightService investmentDivWeightService; // todo - remove post testing

  @EventListener(ApplicationReadyEvent.class)
  public void startup() {
    if (pingConfig.pinger().allEndPointsActive(crdbServiceFeign)) {
      FixedDepositProto.FixedDepositList fixedDepositList =
          crdbServiceFeign.getFixedDeposits(BANK.name(), "CIBC.*");
      System.out.println(fixedDepositList);
    }

    log.info("Startup complete");

    /*Optional<InvestmentDivWeight> optionalInvestmentDivWeight =
        investmentDivWeightService.calcInvestmentBasedOnDivWeight(
            Lists.newArrayList("VFV.TO", "VDY.TO", "VCE.TO", "CCO.TO"),
            Lists.newArrayList(
                "RY.TO", "BNS.TO", "CM.TO", "ENB.TO", "BCE.TO", "TRP.TO", "SU.TO", "CNQ.TO",
                "CP.TO", "PZA.TO", "DBM.TO", "FTS.TO"),
            5000.0,
            20.0,
            0.0);
    System.out.println(optionalInvestmentDivWeight.get());*/
  }
}
