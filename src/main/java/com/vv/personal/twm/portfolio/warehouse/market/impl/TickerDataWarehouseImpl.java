package com.vv.personal.twm.portfolio.warehouse.market.impl;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.vv.personal.twm.portfolio.warehouse.market.TickerDataWarehouse;
import java.time.LocalDate;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Slf4j
@Getter
public class TickerDataWarehouseImpl implements TickerDataWarehouse {

  private final Table<LocalDate, String, Double> adjustedClosePriceTableForAnalysis;
  private final TreeSet<String> instruments;

  public TickerDataWarehouseImpl() {
    this.instruments = new TreeSet<>();
    adjustedClosePriceTableForAnalysis = HashBasedTable.create();
  }

  @Override
  public void put(LocalDate date, String imnt, Double price) {
    adjustedClosePriceTableForAnalysis.put(date, imnt, price);
  }

  @Override
  public Double get(LocalDate date, String imnt) {
    return adjustedClosePriceTableForAnalysis.get(date, imnt);
  }

  @Override
  public List<LocalDate> getDates() {
    return adjustedClosePriceTableForAnalysis.rowKeySet().stream()
        .sorted()
        .collect(Collectors.toList());
  }
}
