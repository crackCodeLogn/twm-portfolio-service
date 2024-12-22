package com.vv.personal.twm.portfolio.util;

import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-12-14
 */
class SanitizerUtilTest {

  @Test
  public void testSanitizeDollar() {
    assertEquals("", SanitizerUtil.sanitizeDollar("$"));
    assertEquals("10.23", SanitizerUtil.sanitizeDollar("$10.23"));
  }

  @Test
  public void testSanitizeDouble() {
    assertEquals(10.23, SanitizerUtil.sanitizeDouble("10.23"), DELTA_PRECISION);
    assertEquals(10.0, SanitizerUtil.sanitizeDouble("10"), DELTA_PRECISION);
    assertEquals(10.23, SanitizerUtil.sanitizeDouble("$10.23"), DELTA_PRECISION);
    assertEquals(10.23, SanitizerUtil.sanitizeDouble("$10.23 "), DELTA_PRECISION);
    assertEquals(10.23, SanitizerUtil.sanitizeDouble(" $10.23 "), DELTA_PRECISION);
    assertEquals(10.23, SanitizerUtil.sanitizeDouble("$ 10.23"), DELTA_PRECISION);
    assertEquals(0, SanitizerUtil.sanitizeDouble("$ 10. 23"), DELTA_PRECISION);
    assertEquals(0, SanitizerUtil.sanitizeDouble(""), DELTA_PRECISION);
  }

  @Test
  public void testSanitizeString() {
    assertEquals("BNS-m", SanitizerUtil.sanitizeString("BNS Manufactured"));
    assertEquals("BNS-m", SanitizerUtil.sanitizeString("BNS manufactured"));
    assertEquals("BNS", SanitizerUtil.sanitizeString("BNS "));
  }
}
