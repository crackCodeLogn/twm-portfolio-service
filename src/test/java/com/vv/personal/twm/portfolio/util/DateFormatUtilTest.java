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
}
