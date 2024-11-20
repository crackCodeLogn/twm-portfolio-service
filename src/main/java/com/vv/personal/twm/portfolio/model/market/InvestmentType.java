package com.vv.personal.twm.portfolio.model.market;

import lombok.Getter;

/**
 * @author Vivek
 * @since 2024-11-11
 */
@Getter
public enum InvestmentType {
  HIGH_VALUE(1),
  HIGH_DIV(2);

  private final int investmentType;

  InvestmentType(int investmentType) {
    this.investmentType = investmentType;
  }
}
