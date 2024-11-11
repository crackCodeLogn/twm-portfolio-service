package com.vv.personal.twm.portfolio.market.warehouse;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.time.LocalDate;
import java.util.TreeSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Slf4j
@Getter
@Setter
public class TickerDataWarehouse {

  private final Table<LocalDate, String, Double> adjustedClosePriceTableForAnalysis;
  private final TreeSet<String> instruments;

  public TickerDataWarehouse() {
    this.instruments = new TreeSet<>();
    adjustedClosePriceTableForAnalysis = HashBasedTable.create();
  }

  public void put(LocalDate date, String imnt, Double price) {
    adjustedClosePriceTableForAnalysis.put(date, imnt, price);
  }

  public Double get(LocalDate date, String imnt) {
    return adjustedClosePriceTableForAnalysis.get(date, imnt);
  }
}
