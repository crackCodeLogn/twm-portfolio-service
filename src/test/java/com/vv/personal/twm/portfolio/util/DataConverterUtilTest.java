package com.vv.personal.twm.portfolio.util;

import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2025-12-14
 */
class DataConverterUtilTest {

  @Test
  void getCorrelationMatrix() {
    Table<String, String, Double> table = HashBasedTable.create();
    table.put("a1", "a1", 0.0);
    table.put("a1", "a2", .895);
    table.put("a1", "a3", .415);
    table.put("a2", "a1", .895);
    table.put("a2", "a2", 0.0);
    table.put("a2", "a3", -.521);
    table.put("a3", "a1", .415);
    table.put("a3", "a2", -.521);
    table.put("a3", "a3", 0.0);

    MarketDataProto.CorrelationMatrix correlationMatrix =
        DataConverterUtil.getCorrelationMatrix(Optional.of(table));
    assertNotNull(correlationMatrix);
    assertEquals(9, correlationMatrix.getEntriesCount());
    MarketDataProto.CorrelationCell entry0 = correlationMatrix.getEntries(0);
    assertEquals("a1", entry0.getImntRow());
    assertEquals("a1", entry0.getImntCol());
    assertEquals(0.0, entry0.getValue(), DELTA_PRECISION);
    MarketDataProto.CorrelationCell entry5 = correlationMatrix.getEntries(5);
    assertEquals("a2", entry5.getImntRow());
    assertEquals("a3", entry5.getImntCol());
    assertEquals(-0.521, entry5.getValue(), DELTA_PRECISION);
  }

  @Test
  void getCorrelationMatrix_Empty() {
    MarketDataProto.CorrelationMatrix correlationMatrix =
        DataConverterUtil.getCorrelationMatrix(Optional.empty());
    assertNotNull(correlationMatrix);
    assertEquals(0, correlationMatrix.getEntriesCount());
  }

  @Test
  void split() {
    String test = "abc,\"dfdf,   fe\", erf,,";
    List<String> list = DataConverterUtil.split(test);
    assertEquals(5, list.size());
    assertEquals("abc", list.get(0));
    assertEquals("dfdf,   fe", list.get(1));
    assertEquals("erf", list.get(2));
    assertTrue(list.get(3).isEmpty());
    assertTrue(list.get(4).isEmpty());
  }

  @Test
  void split_Empty() {
    List<String> list = DataConverterUtil.split(null);
    assertTrue(list.isEmpty());

    list = DataConverterUtil.split("");
    assertTrue(list.isEmpty());
  }
}
