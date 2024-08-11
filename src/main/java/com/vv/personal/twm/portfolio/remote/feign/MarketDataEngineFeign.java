package com.vv.personal.twm.portfolio.remote.feign;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@FeignClient(name = "market-data", url = "localhost:8083")
public interface MarketDataEngineFeign {

    @GetMapping("/mkt/{countryCode}/ticker/name/{symbol}")
    String getTickerName(@PathVariable String countryCode,
                         @PathVariable String symbol);

    @GetMapping("/mkt/{countryCode}/ticker/sector/{symbol}")
    String getTickerSector(@PathVariable String countryCode, @PathVariable String symbol);

    @GetMapping("/proto/mkt?symbol={symbol}&start={start}&end={end}&country={countryCode}")
    MarketDataProto.Ticker getTickerData(@PathVariable String countryCode,
                                         @PathVariable String symbol,
                                         @PathVariable String start,
                                         @PathVariable String end);

    @GetMapping("/proto/mkt?symbol={symbol}&start={start}&end={end}")
    MarketDataProto.Ticker getTickerDataWithoutCountryCode(@PathVariable String symbol,
                                                           @PathVariable String start,
                                                           @PathVariable String end);

    @GetMapping("/proto/mkt/portfolio")
    MarketDataProto.Portfolio getPortfolioData();

}