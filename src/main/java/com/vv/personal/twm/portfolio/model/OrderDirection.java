package com.vv.personal.twm.portfolio.model;

import lombok.Getter;

/**
 * @author Vivek
 * @since 2024-09-07
 */
@Getter
public enum OrderDirection {
  BUY("b"),
  SELL("s");

  private final String direction;

  OrderDirection(String direction) {
    this.direction = direction;
  }

  @Override
  public String toString() {
    return direction;
  }
}
