package com.vv.personal.twm.portfolio.model.market;

import lombok.Getter;

/**
 * @author Vivek
 * @since 2025-02-09
 */
@Getter
public enum NetWorthBreakDownKey {
  NET_WORTH("net-worth"),
  MARKET("market"),
  BANK("bank"),
  GIC("gic"),
  OTHER("other");

  private final String key;

  NetWorthBreakDownKey(String key) {
    this.key = key;
  }
}
