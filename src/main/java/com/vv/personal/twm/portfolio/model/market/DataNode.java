package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Vivek
 * @since 2024-09-11
 */
@Slf4j
@Getter
@Setter
public class DataNode {

  private final MarketDataProto.Instrument instrument;
  private DataNode next;
  private DataNode prev;
  private double runningQuantity;
  private ACB acb;
  private boolean oneTimeProcessed;

  public DataNode(MarketDataProto.Instrument instrument) {
    this.instrument = instrument;
    this.next = null;
    this.prev = null;
    this.acb = null;
    this.runningQuantity = instrument.getQty();
    this.oneTimeProcessed = false;
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
      totalAcb = instrument.getTicker().getData(0).getPrice();

    } else { // consider direction now
      if (instrument.getDirection() == MarketDataProto.Direction.BUY) {
        if (!oneTimeProcessed) {
          runningQuantity += prev.getRunningQuantity();
          oneTimeProcessed = true;
        } else {
          log.warn(
              "Not updating BUY runningQuantity for {} as it has already been processed!",
              instrument.getTicker().getSymbol());
        }

        totalAcb = prev.getAcb().getTotalAcb() + instrument.getTicker().getData(0).getPrice();
      } else { // SELL direction
        if (!oneTimeProcessed) {
          runningQuantity =
              Math.max(0, prev.getRunningQuantity() - runningQuantity); // handle oversell, disallow
          // short selling
          oneTimeProcessed = true;
        } else {
          log.warn(
              "Not updating SELL runningQuantity for {} as it has already been processed!",
              instrument.getTicker().getSymbol());
        }
        totalAcb =
            runningQuantity == 0 // indicates position closure at this point in time
                ? 0.0
                : prev.getAcb().getTotalAcb() - instrument.getTicker().getData(0).getPrice();
      }
    }

    // cannot allow negative, but seems if 0, then tax implications become crazy - future work maybe
    // https://www.adjustedcostbase.ca/blog/can-my-adjusted-cost-base-be-negative/
    totalAcb = Math.max(totalAcb, 0);

    double acbPerShare =
        runningQuantity > 0 ? totalAcb / runningQuantity : 0.0; // looks like position closure if 0
    acb = ACB.builder().totalAcb(totalAcb).acbPerUnit(acbPerShare).build();
  }
}
