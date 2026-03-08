package com.vv.personal.twm.portfolio.util.math;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author Vivek
 * @since 2026-03-07
 */
public final class MathUtil {

  private MathUtil() {}

  public static Double round2(double val) {
    BigDecimal bd = new BigDecimal(Double.toString(val));
    bd = bd.setScale(2, RoundingMode.HALF_UP);
    return bd.doubleValue();
  }
}
