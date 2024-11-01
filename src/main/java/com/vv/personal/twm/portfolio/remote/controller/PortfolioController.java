package com.vv.personal.twm.portfolio.remote.controller;

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
@RestController("portfolio")
@Controller
@RequestMapping("/portfolio/")
@AllArgsConstructor
public class PortfolioController {

  /*@Lazy
  @Qualifier("ticker-dwh-s")
  private final TickerDataWarehouse soldTickerDataWarehouse;

  @Lazy
  @Qualifier("ticker-dwh-b")
  private final TickerDataWarehouse boughtTickerDataWarehouse;*/

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
}
