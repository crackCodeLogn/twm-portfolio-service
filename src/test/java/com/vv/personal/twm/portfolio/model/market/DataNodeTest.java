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
        generateInstrument(10, 50.1, MarketDataProto.Direction.BUY); // this price is the price paid

    DataNode dataNode = new DataNode(instrument);
    dataNode.computeAcb();
    assertNull(dataNode.getPrev());
    assertNull(dataNode.getNext());
    assertNotNull(dataNode.getInstrument());
    assertEquals(10, dataNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(50.1, dataNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.01, dataNode.getAcb().getAcbPerUnit(), DELTA_PRECISION);
  }

  @Test
  public void notNullPreviousDataNodeCreation() {
    MarketDataProto.Instrument firstImnt =
        generateInstrument(10, 50.1, MarketDataProto.Direction.BUY); // this price is the price paid
    MarketDataProto.Instrument secondImnt =
        generateInstrument(20, 100, MarketDataProto.Direction.BUY); // this price is the price paid

    DataNode firstNode = new DataNode(firstImnt);
    DataNode secondNode = new DataNode(secondImnt);

    firstNode.setNext(secondNode);
    secondNode.setPrev(firstNode);

    firstNode.computeAcb();
    secondNode.computeAcb();

    //    System.out.println(firstNode);
    assertEquals(10, firstNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(50.1, firstNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.01, firstNode.getAcb().getAcbPerUnit(), DELTA_PRECISION);

    assertEquals(30, secondNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(150.1, secondNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.0033333333, secondNode.getAcb().getAcbPerUnit(), DELTA_PRECISION);
  }

  @Test
  public void preventMultiComputeAcbInBuy() {
    MarketDataProto.Instrument firstImnt =
        generateInstrument(10, 50.1, MarketDataProto.Direction.BUY); // this price is the price paid
    MarketDataProto.Instrument secondImnt =
        generateInstrument(20, 100, MarketDataProto.Direction.BUY); // this price is the price paid

    DataNode firstNode = new DataNode(firstImnt);
    DataNode secondNode = new DataNode(secondImnt);

    firstNode.setNext(secondNode);
    secondNode.setPrev(firstNode);

    firstNode.computeAcb();
    assertEquals(10, firstNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(50.1, firstNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    secondNode.computeAcb();
    assertEquals(30, secondNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(150.1, secondNode.getAcb().getTotalAcb(), DELTA_PRECISION);

    firstNode.computeAcb();
    assertEquals(10, firstNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(50.1, firstNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    secondNode.computeAcb();
    assertEquals(30, secondNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(150.1, secondNode.getAcb().getTotalAcb(), DELTA_PRECISION);
  }

  @Test
  public void preventMultiComputeAcbInSell() {
    MarketDataProto.Instrument firstImnt =
        generateInstrument(10, 50.1, MarketDataProto.Direction.BUY); // this price is the price paid
    MarketDataProto.Instrument secondImnt =
        generateInstrument(1, 6, MarketDataProto.Direction.SELL); // this price is the price sold at

    DataNode firstNode = new DataNode(firstImnt);
    DataNode secondNode = new DataNode(secondImnt);

    firstNode.setNext(secondNode);
    secondNode.setPrev(firstNode);

    firstNode.computeAcb();
    assertEquals(50.1, firstNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(10, firstNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(5.01, firstNode.getAcb().getAcbPerUnit(), DELTA_PRECISION);
    secondNode.computeAcb();
    assertEquals(9, secondNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(44.1, secondNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(4.9, secondNode.getAcb().getAcbPerUnit(), DELTA_PRECISION);

    secondNode.computeAcb();
    assertEquals(9, secondNode.getRunningQuantity(), DELTA_PRECISION);
    assertEquals(44.1, secondNode.getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(4.9, secondNode.getAcb().getAcbPerUnit(), DELTA_PRECISION);
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
