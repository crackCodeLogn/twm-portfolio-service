package com.vv.personal.twm.mkt.remote;

import com.vv.personal.twm.mkt.market.warehouse.TickerDataWarehouse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Vivek
 * @since 2023-11-23
 */
@Slf4j
@RestController("market")
@Controller
@RequestMapping("/mkt/")
@AllArgsConstructor
public class MarketController {

    private final TickerDataWarehouse tickerDataWarehouse;

    @GetMapping("/")
    public String get() {
        return "hi";
    }

    @GetMapping("/portfolio")
    public void getPortfolio() {
        log.info("Firing get portfolio request");
        tickerDataWarehouse.generateData();
        log.info("get portf request completed");
    }
}
