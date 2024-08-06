package com.vv.personal.twm.mkt.market.warehouse;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.mkt.remote.feign.MarketDataEngineFeign;
import com.vv.personal.twm.mkt.util.DateFormatUtil;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.util.TreeSet;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Slf4j
public class TickerDataWarehouse {

    private final MarketDataEngineFeign marketDataEngineFeign;
    private final Table<LocalDate, String, Double> adjustedClosePriceTable;
    private final Table<LocalDate, String, Double> adjustedClosePriceTableForAnalysis;
    private final TreeSet<String> instruments;
    private final LocalDate startDateOfInvestment;
    private final LocalDate startDateForAnalysis;
    private final LocalDate endDate;

    public TickerDataWarehouse(MarketDataEngineFeign marketDataEngineFeign, TreeSet<String> instruments, LocalDate startDateOfInvestment, LocalDate endDate, LocalDate startDateForAnalysis) {
        this.marketDataEngineFeign = marketDataEngineFeign;
        this.instruments = instruments;
        this.startDateOfInvestment = startDateOfInvestment;
        this.endDate = endDate;
        this.startDateForAnalysis = startDateForAnalysis;

        adjustedClosePriceTable = HashBasedTable.create();
        adjustedClosePriceTableForAnalysis = HashBasedTable.create();
    }

    public void generateData() {
        this.instruments.forEach(imnt -> {
            log.info("Downloading data for {} from {} to {}", imnt, startDateOfInvestment, endDate);
            MarketDataProto.Ticker tickerData = marketDataEngineFeign.getTickerDataWithoutCountryCode(imnt,
                    startDateForAnalysis.toString(),
                    endDate.toString());

            tickerData.getDataList().forEach(data -> {
                LocalDate date = DateFormatUtil.getLocalDate(data.getDate());
                adjustedClosePriceTableForAnalysis.put(date, imnt, data.getPrice());
                if (!date.isBefore(startDateOfInvestment))
                    adjustedClosePriceTable.put(date, imnt, data.getPrice());
            });
        });
        System.out.println(adjustedClosePriceTable);
    }

}
