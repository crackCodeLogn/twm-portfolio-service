package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.Optional;
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
  private Optional<Double> pnl;
  private Optional<Double> pnlSoldQty;
  private boolean isClosingPositionNode;
  private boolean oneTimeProcessed; // helps in enforcing single time calc and not mistaken re-runs

  public DataNode(MarketDataProto.Instrument instrument) {
    this.instrument = instrument;
    this.next = null;
    this.prev = null;
    this.acb = null;
    this.runningQuantity = instrument.getQty();
    this.oneTimeProcessed = false;
    this.pnl = Optional.empty();
    this.pnlSoldQty = Optional.empty();
    this.isClosingPositionNode = false;
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

  /**
   * <a href="https://www.atb.com/wealth/good-advice/tax/understanding-adjusted-cost-base/">Follow
   * logic shown here</a>
   */
  public void computeAcb() {
    double totalAcb, acbU;
    if (prev == null) { // no short selling, so assuming first node will always have BUY direction
      // this price is total buy price transaction (price per share * qty)
      totalAcb = instrument.getTicker().getData(0).getPrice();
      // BUY assumption for first node, direct use of qty > 0
      acbU = totalAcb / runningQuantity;

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
        acbU = totalAcb / runningQuantity;
      } else { // SELL direction
        // IMPORTANT -- ACB/U is NOT updated on SELL, only the total ACB decreases due to qty drop
        acbU = prev.getAcb().getAcbPerUnit();
        // original pnl formula => (price-per-share - acbu) * sold-qty
        // => pps * sold-qty - acbu * sold-qty
        // => sold-price-from-txn - acbu * sold-qty
        pnl = Optional.of(instrument.getTicker().getData(0).getPrice() - acbU * runningQuantity);
        pnlSoldQty = Optional.of(runningQuantity);

        if (!oneTimeProcessed) {
          // handle oversell + position close, disallow short selling
          runningQuantity = Math.max(0, prev.getRunningQuantity() - runningQuantity);
          oneTimeProcessed = true;
        } else {
          log.warn(
              "Not updating SELL runningQuantity for {} as it has already been processed!",
              instrument.getTicker().getSymbol());
        }
        /* INCORRECT LOGIC
        totalAcb =
            runningQuantity == 0 // indicates position closure at this point in time
                ? 0.0
                : prev.getAcb().getTotalAcb() - instrument.getTicker().getData(0).getPrice();*/
        if (runningQuantity == 0.0) acbU = 0.0; // closing position
        totalAcb = runningQuantity * acbU; // updated r-qty, and prevents any <= 0 incorrect calc
      }
    }
    acb = ACB.builder().totalAcb(totalAcb).acbPerUnit(acbU).build();
    if (runningQuantity == 0.0 && next == null) {
      setClosingPositionNode(true);
      log.info(
          "Marked node for imnt {} x {} as closed on {}",
          instrument.getTicker().getSymbol(),
          instrument.getAccountType(),
          instrument.getTicker().getData(0).getDate());
    }

    /* INCORRECT LOGIC
    // cannot allow negative, but seems if 0, then tax implications become crazy - future work maybe
    // https://www.adjustedcostbase.ca/blog/can-my-adjusted-cost-base-be-negative/
    totalAcb = Math.max(totalAcb, 0);
    double acbPerShare =
        runningQuantity > 0 ? totalAcb / runningQuantity : 0.0; // looks like position closure if 0 */
  }
}
