package com.vv.personal.twm.portfolio.remote.feign;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.ping.remote.feign.PingFeign;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author Vivek
 * @since 2024-08-11
 */
@FeignClient("twm-market-data-crdb-service")
public interface MarketDataCrdbServiceFeign extends PingFeign {

  @GetMapping("/crdb/mkt/data/{ticker}")
  MarketDataProto.Ticker getMarketDataByTicker(@PathVariable("ticker") String ticker);

  @GetMapping("/crdb/mkt/data/")
  MarketDataProto.Ticker getEntireMarketData();

  @PostMapping("/crdb/mkt/data-single-ticker/")
  String addMarketDataForSingleTicker(@RequestBody MarketDataProto.Ticker ticker);

  @PostMapping("/crdb/mkt/data/")
  String addMarketData(@RequestBody MarketDataProto.Portfolio portfolio);
}
