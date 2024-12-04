package com.vv.personal.twm.portfolio.market.warehouse;

import java.time.LocalDate;
import java.util.List;

/**
 * @author Vivek
 * @since 2023-11-24
 */
public interface TickerDataWarehouse {

  void put(LocalDate date, String imnt, Double price);

  Double get(LocalDate date, String imnt);

  List<LocalDate> getDates();
}
