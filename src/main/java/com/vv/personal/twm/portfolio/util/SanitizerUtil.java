package com.vv.personal.twm.portfolio.util;

import org.apache.commons.lang3.math.NumberUtils;

/**
 * @author Vivek
 * @since 2024-12-14
 */
public final class SanitizerUtil {

  private SanitizerUtil() {}

  public static String sanitizeDollar(String input) {
    return sanitizeString(input).replaceAll("\\$", "");
  }

  public static double sanitizeDouble(String input) {
    input = sanitizeString(sanitizeDollar(input));
    if (NumberUtils.isCreatable(input.trim())) return Double.parseDouble(input);
    return 0.0;
  }

  public static double sanitizeDouble(Double input) {
    return input == null ? 0.0 : input;
  }

  public static String sanitizeString(String input) {
    return input.replaceAll(" [Mm]anufactured", "-m").strip();
  }
}
