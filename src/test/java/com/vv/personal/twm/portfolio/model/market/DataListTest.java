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
        TestInstrument.builder()
            .qty(10)
            .price(50.1)
            .date(20240909)
            .build()
            .getInstrument()); // this price is the price paid
    dataList.addBlock(
        TestInstrument.builder()
            .qty(20)
            .price(100)
            .date(20240912)
            .build()
            .getInstrument()); // this price is the price paid
    dataList.addBlock(
        TestInstrument.builder()
            .qty(30)
            .price(120) // this price is the price sold at
            .direction(SELL)
            .date(20240913)
            .build()
            .getInstrument());
    dataList.computeAcb();

    dataList.display();

    List<DataNode> result = dataList.getData();

    assertEquals(10, result.get(0).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(50.1, result.get(0).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.01, result.get(0).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(0).getInstrument().getDirection());

    assertEquals(30, result.get(1).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(150.1, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.0033333, result.get(1).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());

    assertEquals(0, result.get(2).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(0, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(0, result.get(2).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());
    assertEquals(dataList.getTail(), result.get(2));
  }

  @Test
  public void dataListCreationWith4Blocks_3B_1S() {
    DataList dataList = new DataList();
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(50.1).date(20240909).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(20).price(100).date(20240912).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(150).date(20240920).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder()
            .qty(5)
            .price(120)
            .direction(SELL)
            .date(20240913)
            .build()
            .getInstrument());
    dataList.computeAcb();

    dataList.display();

    List<DataNode> result = dataList.getData();

    assertEquals(10, result.get(0).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(50.1, result.get(0).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.01, result.get(0).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(0).getInstrument().getDirection());

    assertEquals(30, result.get(1).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(150.1, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.0033333, result.get(1).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());

    assertEquals(25, result.get(2).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(30.1, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(1.204, result.get(2).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());

    assertEquals(35, result.get(3).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(180.1, result.get(3).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.145714, result.get(3).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(3).getInstrument().getDirection());
    assertEquals(dataList.getTail(), result.get(3));
  }

  @Test
  public void dataListCreationWith4Blocks_3B_1S_2() {
    // references reference/acb-compute.png

    DataList dataList = new DataList();
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(50.1).date(20240909).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(50.3).date(20240910).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder()
            .qty(5)
            .price(70)
            .date(20240913)
            .direction(SELL)
            .build()
            .getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(10).price(90).date(20240920).build().getInstrument());

    dataList.computeAcb();

    dataList.display();

    List<DataNode> result = dataList.getData();

    assertEquals(10, result.get(0).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(50.1, result.get(0).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.01, result.get(0).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(0).getInstrument().getDirection());

    assertEquals(20, result.get(1).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(100.4, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.02, result.get(1).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());

    assertEquals(15, result.get(2).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(30.4, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(2.0266666, result.get(2).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());

    assertEquals(25, result.get(3).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(120.4, result.get(3).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(4.816, result.get(3).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(3).getInstrument().getDirection());
    assertEquals(dataList.getTail(), result.get(3));
  }
}
