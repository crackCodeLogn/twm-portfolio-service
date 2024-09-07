package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.ping.config.PingConfig;
import com.vv.personal.twm.portfolio.remote.feign.BankCrdbServiceFeign;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import static com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto.FilterBy.BANK;

/**
 * @author Vivek
 * @since 2024-08-11
 */
@Slf4j
@Service
@AllArgsConstructor
public class StartupService {

    private final PingConfig pingConfig;
    private final BankCrdbServiceFeign crdbServiceFeign;

    @EventListener(ApplicationReadyEvent.class)
    public void startup() {
        if (pingConfig.pinger().allEndPointsActive(crdbServiceFeign)) {

            FixedDepositProto.FixedDepositList fixedDepositList = crdbServiceFeign.getFixedDeposits(BANK.name(), "CIBC.*");
            System.out.println(fixedDepositList);
        }

        log.info("Startup complete");
    }
}
