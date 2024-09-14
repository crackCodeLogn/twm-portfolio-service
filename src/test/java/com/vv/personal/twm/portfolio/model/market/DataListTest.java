package com.vv.personal.twm.portfolio.model.market;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.BUY;
import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.SELL;
import static com.vv.personal.twm.portfolio.TestConstants.PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-09-11
 */
class DataListTest {

  @Test
  public void dataListCreation() {
    MarketDataProto.Instrument firstImnt = generateInstrument(10, 5.1, BUY, 20240909);
    MarketDataProto.Instrument secondImnt = generateInstrument(20, 10, BUY, 20240912);
    MarketDataProto.Instrument thirdImnt =
        generateInstrument(5, 12, MarketDataProto.Direction.SELL, 20240913);
    MarketDataProto.Instrument fourthImnt = generateInstrument(10, 15, BUY, 20240920);

    DataList dataList = new DataList();
    dataList.addBlock(firstImnt);
    dataList.addBlock(secondImnt);
    dataList.addBlock(fourthImnt);
    dataList.addBlock(thirdImnt);
    dataList.computeAcb();

    dataList.display();

    List<DataNode> result = dataList.getData();

    assertEquals(10, result.get(0).getRunningQuantity(), PRECISION);
    assertEquals(51, result.get(0).getAcb().getTotalAcb(), PRECISION);
    assertEquals(5.1, result.get(0).getAcb().getAcbPerShare(), PRECISION);
    assertEquals(BUY, result.get(0).getInstrument().getDirection());

    assertEquals(30, result.get(1).getRunningQuantity(), PRECISION);
    assertEquals(251, result.get(1).getAcb().getTotalAcb(), PRECISION);
    assertEquals(8.3666666, result.get(1).getAcb().getAcbPerShare(), PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());

    assertEquals(25, result.get(2).getRunningQuantity(), PRECISION);
    assertEquals(191, result.get(2).getAcb().getTotalAcb(), PRECISION);
    assertEquals(7.64, result.get(2).getAcb().getAcbPerShare(), PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());

    assertEquals(35, result.get(3).getRunningQuantity(), PRECISION);
    assertEquals(341, result.get(3).getAcb().getTotalAcb(), PRECISION);
    assertEquals(9.742857143, result.get(3).getAcb().getAcbPerShare(), PRECISION);
    assertEquals(BUY, result.get(3).getInstrument().getDirection());
  }

  private MarketDataProto.Instrument generateInstrument(
      double qty, double price, MarketDataProto.Direction direction, int date) {
    return MarketDataProto.Instrument.newBuilder()
        .setQty(qty)
        .setDirection(direction)
        .setTicker(
            MarketDataProto.Ticker.newBuilder()
                .setName("test")
                .addData(MarketDataProto.Value.newBuilder().setDate(date).setPrice(price).build())
                .build())
        .build();
  }
}
