package com.vv.personal.twm.portfolio.model;

import static com.vv.personal.twm.portfolio.TestConstants.PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-09-11
 */
class DataListTest {

  @Test
  public void dataListCreation() {
    MarketDataProto.Instrument firstImnt =
        generateInstrument(10, 5.1, MarketDataProto.Direction.BUY);
    MarketDataProto.Instrument secondImnt =
        generateInstrument(20, 10, MarketDataProto.Direction.BUY);
    MarketDataProto.Instrument thirdImnt =
        generateInstrument(5, 12, MarketDataProto.Direction.SELL);
    MarketDataProto.Instrument fourthImnt =
        generateInstrument(10, 15, MarketDataProto.Direction.BUY);

    DataList dataList = new DataList();
    dataList.addBlock(firstImnt);
    dataList.addBlock(secondImnt);
    dataList.addBlock(thirdImnt);
    dataList.addBlock(fourthImnt);

    dataList.display();

    assertEquals(10, dataList.getHead().getRunningQuantity(), PRECISION);
    assertEquals(51, dataList.getHead().getAcb().getTotalAcb(), PRECISION);
    assertEquals(5.1, dataList.getHead().getAcb().getAcbPerShare(), PRECISION);

    assertEquals(30, dataList.getHead().getNext().getRunningQuantity(), PRECISION);
    assertEquals(251, dataList.getHead().getNext().getAcb().getTotalAcb(), PRECISION);
    assertEquals(8.3666666, dataList.getHead().getNext().getAcb().getAcbPerShare(), PRECISION);

    assertEquals(25, dataList.getTail().getPrev().getRunningQuantity(), PRECISION);
    assertEquals(191, dataList.getTail().getPrev().getAcb().getTotalAcb(), PRECISION);
    assertEquals(7.64, dataList.getTail().getPrev().getAcb().getAcbPerShare(), PRECISION);

    assertEquals(35, dataList.getTail().getRunningQuantity(), PRECISION);
    assertEquals(341, dataList.getTail().getAcb().getTotalAcb(), PRECISION);
    assertEquals(9.742857143, dataList.getTail().getAcb().getAcbPerShare(), PRECISION);
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
