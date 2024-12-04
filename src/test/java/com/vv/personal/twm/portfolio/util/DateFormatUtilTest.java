package com.vv.personal.twm.portfolio.util;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-09-10
 */
class DateFormatUtilTest {

  @Test
  void getLocalDate() {
    LocalDate result;

    result = DateFormatUtil.getLocalDate("20240910");
    assertEquals(LocalDate.of(2024, 9, 10), result);

    result = DateFormatUtil.getLocalDate("2024-09-10");
    assertEquals(LocalDate.of(2024, 9, 10), result);

    result = DateFormatUtil.getLocalDate("9/10/2024");
    assertEquals(LocalDate.of(2024, 9, 10), result);

    result = DateFormatUtil.getLocalDate("09/10/2024");
    assertEquals(LocalDate.of(2024, 9, 10), result);

    result = DateFormatUtil.getLocalDate("9/3/2024");
    assertEquals(LocalDate.of(2024, 9, 3), result);

    result = DateFormatUtil.getLocalDate("09/03/2024");
    assertEquals(LocalDate.of(2024, 9, 3), result);
  }

  @Test
  void testGetLocalDate_Int_To_LocalDate() {
    LocalDate result;

    result = DateFormatUtil.getLocalDate(20241203);
    assertEquals("2024-12-03", result.toString());

    result = DateFormatUtil.getLocalDate(20250220);
    assertEquals("2025-02-20", result.toString());
  }

  @Test
  void testGetLocalDate_LocalDate_To_Int() {
    int result;

    result = DateFormatUtil.getLocalDate(LocalDate.of(2024, 12, 3));
    assertEquals(20241203, result);

    result = DateFormatUtil.getLocalDate(LocalDate.of(2025, 2, 20));
    assertEquals(20250220, result);
  }
}
