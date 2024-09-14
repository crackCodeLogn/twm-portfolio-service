package com.vv.personal.twm.portfolio.model.market;

import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-09-11
 */
class DataNodeTest {

  @Test
  public void firstDataNodeCreation() {
    MarketDataProto.Instrument instrument =
        generateInstrument(10, 5.1, MarketDataProto.Direction.BUY);

    DataNode dataNode = new DataNode(instrument);
    dataNode.computeAcb();
    assertNull(dataNode.getPrev());
    assertNull(dataNode.getNext());
    assertNotNull(dataNode.getInstrument());
    assertEquals(10, dataNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(5.1, dataNode.getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(51, dataNode.getAcb().getTotalAcb(), DELTA_PRECISION);
  }

  @Test
  public void notNullPreviousDataNodeCreation() {
    MarketDataProto.Instrument firstImnt =
        generateInstrument(10, 5.1, MarketDataProto.Direction.BUY);
    MarketDataProto.Instrument secondImnt =
        generateInstrument(20, 10, MarketDataProto.Direction.BUY);

    DataNode firstNode = new DataNode(firstImnt);
    DataNode secondNode = new DataNode(secondImnt);

    firstNode.setNext(secondNode);
    secondNode.setPrev(firstNode);

    firstNode.computeAcb();
    secondNode.computeAcb();

    //    System.out.println(firstNode);
    assertEquals(10, firstNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(51, firstNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.1, firstNode.getAcb().getAcbPerShare(), DELTA_PRECISION);

    assertEquals(30, secondNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(251, secondNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(8.3666666, secondNode.getAcb().getAcbPerShare(), DELTA_PRECISION);
  }

  private MarketDataProto.Instrument generateInstrument(
      double qty, double price, MarketDataProto.Direction direction) {
    return MarketDataProto.Instrument.newBuilder()
        .setQty(qty)
        .setDirection(direction)
        .setTicker(
            MarketDataProto.Ticker.newBuilder()
                .setName("test")
                .addData(
                    MarketDataProto.Value.newBuilder().setDate(20240911).setPrice(price).build())
                .build())
        .build();
  }
}
