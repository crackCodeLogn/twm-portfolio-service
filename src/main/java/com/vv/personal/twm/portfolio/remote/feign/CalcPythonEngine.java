package com.vv.personal.twm.portfolio.remote.feign;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.ping.remote.feign.PingFeign;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author Vivek
 * @since 2026-01-16
 */
@FeignClient("twm-calc-py-engine")
public interface CalcPythonEngine extends PingFeign {

  @PostMapping("/calc/portfolio/optimizer")
  MarketDataProto.Portfolio calcPortfolioOptimizer(
      @RequestBody MarketDataProto.Portfolio requestPortfolio);
}
