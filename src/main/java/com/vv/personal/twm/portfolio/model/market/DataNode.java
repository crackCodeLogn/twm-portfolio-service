package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import lombok.*;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Vivek
 * @since 2024-09-11
 */
@Getter
@Setter
public class DataNode {

  private final MarketDataProto.Instrument instrument;
  private DataNode next;
  private DataNode prev;
  private double runningQuantity;
  private ACB acb;

  public DataNode(MarketDataProto.Instrument instrument) {
    this.instrument = instrument;
    this.next = null;
    this.prev = null;
    this.acb = null;
    this.runningQuantity = instrument.getQty();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("instrument", instrument)
        .append("next", next == null ? null : Integer.toHexString(next.hashCode()))
        .append("prev", prev == null ? null : Integer.toHexString(prev.hashCode()))
        .append("runningQuantity", runningQuantity)
        .append("acb", acb)
        .toString();
  }

  public void computeAcb() {
    double totalAcb;
    if (prev == null) { // no short selling, so assuming first node will always have BUY direction
      totalAcb = runningQuantity * instrument.getTicker().getData(0).getPrice();

    } else { // consider direction now
      if (instrument.getDirection() == MarketDataProto.Direction.BUY) {
        runningQuantity += prev.getRunningQuantity();
        totalAcb =
            prev.getAcb().getTotalAcb()
                + (instrument.getQty() * instrument.getTicker().getData(0).getPrice());
      } else {
        runningQuantity = prev.getRunningQuantity() - runningQuantity;
        totalAcb =
            prev.getAcb().getTotalAcb()
                - (instrument.getQty() * instrument.getTicker().getData(0).getPrice());
      }
    }

    acb = ACB.builder().totalAcb(totalAcb).acbPerShare(totalAcb / runningQuantity).build();
  }
}
