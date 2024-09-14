package com.vv.personal.twm.portfolio.model.market;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.BUY;
import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.SELL;
import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.portfolio.util.TestInstrument;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-09-11
 */
class DataListTest {

  @Test
  public void dataListCreationWith3Blocks_2B_1S_closure() {
    DataList dataList = new DataList();
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(5.1).date(20240909).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(20).price(10).date(20240912).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder()
            .qty(30)
            .price(12)
            .direction(SELL)
            .date(20240913)
            .build()
            .getInstrument());
    dataList.computeAcb();

    dataList.display();

    List<DataNode> result = dataList.getData();

    assertEquals(10, result.get(0).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(51, result.get(0).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.1, result.get(0).getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(BUY, result.get(0).getInstrument().getDirection());

    assertEquals(30, result.get(1).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(251, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(8.3666666, result.get(1).getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());

    assertEquals(0, result.get(2).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(0, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(0, result.get(2).getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());
    assertEquals(dataList.getTail(), result.get(2));
  }

  @Test
  public void dataListCreationWith4Blocks_3B_1S() {
    DataList dataList = new DataList();
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(5.1).date(20240909).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(20).price(10).date(20240912).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(15).date(20240920).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder()
            .qty(5)
            .price(12)
            .direction(SELL)
            .date(20240913)
            .build()
            .getInstrument());
    dataList.computeAcb();

    dataList.display();

    List<DataNode> result = dataList.getData();

    assertEquals(10, result.get(0).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(51, result.get(0).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.1, result.get(0).getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(BUY, result.get(0).getInstrument().getDirection());

    assertEquals(30, result.get(1).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(251, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(8.3666666, result.get(1).getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());

    assertEquals(25, result.get(2).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(191, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(7.64, result.get(2).getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());

    assertEquals(35, result.get(3).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(341, result.get(3).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(9.742857143, result.get(3).getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(BUY, result.get(3).getInstrument().getDirection());
    assertEquals(dataList.getTail(), result.get(3));
  }
}
