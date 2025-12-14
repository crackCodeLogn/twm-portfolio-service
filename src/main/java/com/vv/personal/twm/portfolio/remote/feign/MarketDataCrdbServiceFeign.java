package com.vv.personal.twm.portfolio.remote.feign;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.ping.remote.feign.PingFeign;
import java.util.List;
import java.util.Optional;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.DeleteMapping;
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

  @GetMapping("/crdb/mkt/data/{ticker}/{limit}")
  Optional<MarketDataProto.Portfolio> getLimitedDataByTicker(
      @PathVariable("ticker") String ticker, @PathVariable("limit") int limit);

  @PostMapping("/crdb/mkt/data-single-ticker/")
  String addMarketDataForSingleTicker(@RequestBody MarketDataProto.Ticker ticker);

  @PostMapping("/crdb/mkt/data/")
  String addMarketData(@RequestBody MarketDataProto.Portfolio portfolio);

  @DeleteMapping("/crdb/mkt/data/{ticker}/{date}")
  String deleteMarketData(@PathVariable("ticker") String ticker, @PathVariable("date") int date);

  @PostMapping("/crdb/mkt/data/{ticker}/dates")
  String deleteMarketData(@PathVariable("ticker") String ticker, @RequestBody List<Integer> dates);

  @GetMapping("/crdb/mkt/transactions/{direction}")
  Optional<MarketDataProto.Portfolio> getTransactions(@PathVariable("direction") String direction);

  @GetMapping("/crdb/mkt/transactions/dividends/{accountType}")
  Optional<MarketDataProto.Portfolio> getDividends(@PathVariable("accountType") String accountType);

  @PostMapping("/crdb/mkt/transactions/")
  String postTransactions(MarketDataProto.Portfolio portfolio);
}
