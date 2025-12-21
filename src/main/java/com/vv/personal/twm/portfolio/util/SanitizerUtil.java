package com.vv.personal.twm.portfolio.util;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import org.apache.commons.lang3.StringUtils;
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

  public static String sanitizeAndFormat2Double(Double input) {
    return input == null ? "0.0" : String.format("%.02f", input);
  }

  public static String sanitizeString(String input) {
    return StringUtils.isEmpty(input) ? EMPTY : input.replaceAll(" [Mm]anufactured", "-m").strip();
  }

  public static String sanitizeSector(String input) {
    return StringUtils.isEmpty(input)
        ? EMPTY
        : input.toLowerCase().strip().replaceAll("\\s+", EMPTY);
  }

  public static String sanitizeStringOnLength(String input, int length) {
    return StringUtils.isEmpty(input)
        ? EMPTY
        : input.substring(0, Math.min(length, input.length()));
  }
}
