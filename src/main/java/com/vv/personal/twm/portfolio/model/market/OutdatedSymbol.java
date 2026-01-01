package com.vv.personal.twm.portfolio.model.market;

/**
 * If outdateEndDate is -1, then ticker is to be outdated from outdateStartDate onwards, * otherwise
 * outdate status is from outdateStartDate to outdateEndDate
 *
 * @author Vivek
 * @since 2024-12-06
 */
public record OutdatedSymbol(int outdateStartDate, int outdateEndDate)
    implements Comparable<OutdatedSymbol> {

  @Override
  public int compareTo(OutdatedSymbol o) {
    return this.outdateStartDate - o.outdateStartDate;
    /*
    // Do not use this, as we only need the start date for comparison
    return outdateStartDate != o.outdateStartDate
        ? outdateStartDate - o.outdateStartDate
        : outdateEndDate - o.outdateEndDate;*/
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof OutdatedSymbol other) {
      return outdateStartDate == other.outdateStartDate && outdateEndDate == other.outdateEndDate;
    }
    return false;
  }
}
