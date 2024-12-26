package com.vv.personal.twm.portfolio.warehouse.market.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.portfolio.TestConstants;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.warehouse.market.TickerDataWarehouse;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-12-03
 */
class TickerDataWarehouseImplTest {

  private TickerDataWarehouse tickerDataWarehouse;

  @BeforeEach
  void setUp() {
    tickerDataWarehouse = new TickerDataWarehouseImpl();
  }

  @Test
  public void testGetPut() {
    assertNull(tickerDataWarehouse.get(DateFormatUtil.getLocalDate(20241203), "CM.TO"));

    tickerDataWarehouse.put(DateFormatUtil.getLocalDate(20241203), "CM.TO", 81.29);
    tickerDataWarehouse.put(DateFormatUtil.getLocalDate(20241202), "CM.TO", 80.29);
    tickerDataWarehouse.put(DateFormatUtil.getLocalDate(20241204), "BNS.TO", 110.94);

    assertEquals(
        80.29,
        tickerDataWarehouse.get(DateFormatUtil.getLocalDate(20241202), "CM.TO"),
        TestConstants.DELTA_PRECISION);
    assertEquals(
        81.29,
        tickerDataWarehouse.get(DateFormatUtil.getLocalDate(20241203), "CM.TO"),
        TestConstants.DELTA_PRECISION);
    assertEquals(
        110.94,
        tickerDataWarehouse.get(DateFormatUtil.getLocalDate(20241204), "BNS.TO"),
        TestConstants.DELTA_PRECISION);

    assertNull(tickerDataWarehouse.get(DateFormatUtil.getLocalDate(20241201), "CM.TO"));
  }

  @Test
  public void testGetDates() {
    assertTrue(tickerDataWarehouse.getDates().isEmpty());

    tickerDataWarehouse.put(DateFormatUtil.getLocalDate(20241203), "CM.TO", 81.29);
    tickerDataWarehouse.put(DateFormatUtil.getLocalDate(20241202), "CM.TO", 80.29);
    tickerDataWarehouse.put(DateFormatUtil.getLocalDate(20241204), "BNS.TO", 110.94);

    List<LocalDate> dates = tickerDataWarehouse.getDates();
    assertNotNull(dates);
    assertEquals(3, dates.size());
    assertEquals(20241202, DateFormatUtil.getDate(dates.get(0)));
    assertEquals(20241203, DateFormatUtil.getDate(dates.get(1)));
    assertEquals(20241204, DateFormatUtil.getDate(dates.get(2)));
  }
}
