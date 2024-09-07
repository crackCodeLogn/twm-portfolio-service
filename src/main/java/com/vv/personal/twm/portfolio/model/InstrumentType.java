package com.vv.personal.twm.portfolio.model;

import lombok.Getter;

/**
 * @author Vivek
 * @since 2023-11-24
 */
@Getter
public enum InstrumentType {
  STK(1),
  ETF(2),
  GIC(3),
  CRYPTO(4);

  private final int imntType;

  InstrumentType(int imntType) {
    this.imntType = imntType;
  }
}
