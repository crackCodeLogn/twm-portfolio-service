package com.vv.personal.twm.portfolio.util.math;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2026-03-07
 */
class MathUtilTest {

  @Test
  public void round2() {
    assertEquals(23.45, MathUtil.round2(23.451224), 0);
    assertEquals(23.0, MathUtil.round2(23.0), 0);
    assertEquals(23.98, MathUtil.round2(23.97999999), 0);
  }
}
