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

  @EventListener(ApplicationReadyEvent.class)
  public void startup() {
    // load analysis data for imnts which are bought
    tickerDataWarehouseService.loadAnalysisDataForInstruments(completeMarketData.getInstruments());

    if (pingConfig.pinger().allEndPointsActive(crdbServiceFeign)) {
      FixedDepositProto.FixedDepositList fixedDepositList =
          crdbServiceFeign.getFixedDeposits(BANK.name(), "CIBC.*");
      System.out.println(fixedDepositList);
    }

    log.info("Startup complete");
  }
}
