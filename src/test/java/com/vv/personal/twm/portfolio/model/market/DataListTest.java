package com.vv.personal.twm.portfolio.model.market;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.BUY;
import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.SELL;
import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION2;
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
    assertTrue(result.get(0).getPnl().isEmpty());

    assertEquals(30, result.get(1).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(150.1, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.0033333, result.get(1).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());
    assertTrue(result.get(1).getPnl().isEmpty());

    assertEquals(25, result.get(2).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(125.0833325, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.0033333, result.get(2).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());
    assertFalse(result.get(2).getPnl().isEmpty());
    assertEquals(94.9833335, result.get(2).getPnl().get(), DELTA_PRECISION);

    assertEquals(35, result.get(3).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(275.0833325, result.get(3).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(7.859523786, result.get(3).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(3).getInstrument().getDirection());
    assertTrue(result.get(3).getPnl().isEmpty());
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
    assertTrue(result.get(0).getPnl().isEmpty());

    assertEquals(20, result.get(1).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(100.4, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.02, result.get(1).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());
    assertTrue(result.get(1).getPnl().isEmpty());

    assertEquals(15, result.get(2).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(75.3, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5.02, result.get(2).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(SELL, result.get(2).getInstrument().getDirection());
    assertFalse(result.get(2).getPnl().isEmpty());
    assertEquals(44.9, result.get(2).getPnl().get(), DELTA_PRECISION);

    assertEquals(25, result.get(3).getRunningQuantity(), DELTA_PRECISION);
    assertEquals(165.3, result.get(3).getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(6.612, result.get(3).getAcb().getAcbPerUnit(), DELTA_PRECISION);
    assertEquals(BUY, result.get(3).getInstrument().getDirection());
    assertTrue(result.get(3).getPnl().isEmpty());
    assertEquals(dataList.getTail(), result.get(3));
  }

  @Test
  public void dataListCreationWith6Blocks_4B_2S() {
    // refer: https://www.atb.com/wealth/good-advice/tax/understanding-adjusted-cost-base/

    DataList dataList = new DataList();
    dataList.addBlock(
        TestInstrument.builder().qty(100).price(3000).date(20260307).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(95).price(3049.50).date(20260308).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(60).price(2215.20).date(20260309).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder()
            .qty(20)
            .price(679.20)
            .date(20260310)
            .direction(SELL)
            .build()
            .getInstrument());
    dataList.addBlock(
        TestInstrument.builder().qty(110).price(4407.70).date(20260311).build().getInstrument());
    dataList.addBlock(
        TestInstrument.builder()
            .qty(22)
            .price(842.38)
            .date(20260312)
            .direction(SELL)
            .build()
            .getInstrument());
    dataList.computeAcb();

    dataList.display();

    List<DataNode> result = dataList.getData();

    assertEquals(100, result.get(0).getRunningQuantity(), DELTA_PRECISION2);
    assertEquals(3000, result.get(0).getAcb().getTotalAcb(), DELTA_PRECISION2);
    assertEquals(30, result.get(0).getAcb().getAcbPerUnit(), DELTA_PRECISION2);
    assertEquals(BUY, result.get(0).getInstrument().getDirection());
    assertTrue(result.get(0).getPnl().isEmpty());
    assertTrue(result.get(0).getPnlSoldQty().isEmpty());

    assertEquals(195, result.get(1).getRunningQuantity(), DELTA_PRECISION2);
    assertEquals(6049.50, result.get(1).getAcb().getTotalAcb(), DELTA_PRECISION2);
    assertEquals(31.02, result.get(1).getAcb().getAcbPerUnit(), DELTA_PRECISION2);
    assertEquals(BUY, result.get(1).getInstrument().getDirection());
    assertTrue(result.get(1).getPnl().isEmpty());
    assertTrue(result.get(1).getPnlSoldQty().isEmpty());

    assertEquals(255, result.get(2).getRunningQuantity(), DELTA_PRECISION2);
    assertEquals(8264.70, result.get(2).getAcb().getTotalAcb(), DELTA_PRECISION2);
    assertEquals(32.41, result.get(2).getAcb().getAcbPerUnit(), DELTA_PRECISION2);
    assertEquals(BUY, result.get(2).getInstrument().getDirection());
    assertTrue(result.get(2).getPnl().isEmpty());
    assertTrue(result.get(2).getPnlSoldQty().isEmpty());

    assertEquals(235, result.get(3).getRunningQuantity(), DELTA_PRECISION2);
    assertEquals(7616.488, result.get(3).getAcb().getTotalAcb(), DELTA_PRECISION2); // 7616.50
    assertEquals(32.41, result.get(3).getAcb().getAcbPerUnit(), DELTA_PRECISION2);
    assertEquals(SELL, result.get(3).getInstrument().getDirection());
    assertFalse(result.get(3).getPnl().isEmpty());
    assertEquals(30.98, result.get(3).getPnl().get(), DELTA_PRECISION2); // 31
    assertFalse(result.get(3).getPnlSoldQty().isEmpty());
    assertEquals(20, result.get(3).getPnlSoldQty().get(), DELTA_PRECISION2);

    assertEquals(345, result.get(4).getRunningQuantity(), DELTA_PRECISION2);
    assertEquals(12024.188, result.get(4).getAcb().getTotalAcb(), DELTA_PRECISION2); // 12024.20
    assertEquals(34.85, result.get(4).getAcb().getAcbPerUnit(), DELTA_PRECISION2);
    assertEquals(BUY, result.get(4).getInstrument().getDirection());
    assertTrue(result.get(4).getPnl().isEmpty());
    assertTrue(result.get(4).getPnlSoldQty().isEmpty());

    assertEquals(323, result.get(5).getRunningQuantity(), DELTA_PRECISION2);
    assertEquals(11257.428, result.get(5).getAcb().getTotalAcb(), DELTA_PRECISION2); // 11257.50
    assertEquals(34.85, result.get(5).getAcb().getAcbPerUnit(), DELTA_PRECISION2);
    assertEquals(SELL, result.get(5).getInstrument().getDirection());
    assertFalse(result.get(5).getPnl().isEmpty());
    assertEquals(75.6201, result.get(5).getPnl().get(), DELTA_PRECISION2); // 75.68
    assertFalse(result.get(5).getPnlSoldQty().isEmpty());
    assertEquals(22, result.get(5).getPnlSoldQty().get(), DELTA_PRECISION2);
  }
}
