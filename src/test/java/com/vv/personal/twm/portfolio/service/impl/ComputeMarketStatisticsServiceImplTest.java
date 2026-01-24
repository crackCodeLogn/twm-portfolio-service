package com.vv.personal.twm.portfolio.service.impl;

import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Vivek
 * @since 2025-12-01
 */
@ExtendWith(MockitoExtension.class)
class ComputeMarketStatisticsServiceImplTest {

  @Mock private TickerDataWarehouseService tickerDataWarehouseService;
  private ComputeMarketStatisticsServiceImpl computeStatisticsServiceImpl;

  @BeforeEach
  void setUp() {
    computeStatisticsServiceImpl =
        new ComputeMarketStatisticsServiceImpl(
            new DateLocalDateCache(), tickerDataWarehouseService);
  }

  @AfterEach
  void tearDown() {
    computeStatisticsServiceImpl.cleanUp();
  }

  @Test
  void calculateCorrelation_AssumingAllAssumptions() {
    when(tickerDataWarehouseService.getMarketData("z.to", 20251201)).thenReturn(10.0);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251202)).thenReturn(12.0);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251203)).thenReturn(11.0);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251204)).thenReturn(13.0);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251205)).thenReturn(15.0);

    when(tickerDataWarehouseService.getMarketData("v.to", 20251201)).thenReturn(20.0);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251202)).thenReturn(21.0);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251203)).thenReturn(23.0);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251204)).thenReturn(22.0);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251205)).thenReturn(24.0);

    List<Integer> dates = Lists.newArrayList(20251201, 20251202, 20251203, 20251204, 20251205);

    Optional<Double> correlation =
        computeStatisticsServiceImpl.computeCorrelation("v.to", "z.to", dates);
    assertTrue(correlation.isPresent());
    assertEquals(-0.521633, correlation.get(), DELTA_PRECISION);
  }

  @Test
  void calculateCorrelation_AssumingAllAssumptions2() {
    when(tickerDataWarehouseService.getMarketData("z.to", 20251201)).thenReturn(10.20);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251202)).thenReturn(10.25);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251203)).thenReturn(10.10);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251204)).thenReturn(11.50);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251205)).thenReturn(12.30);

    when(tickerDataWarehouseService.getMarketData("v.to", 20251201)).thenReturn(102.20);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251202)).thenReturn(104.25);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251203)).thenReturn(99.10);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251204)).thenReturn(96.99);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251205)).thenReturn(101.55);

    List<Integer> dates = Lists.newArrayList(20251201, 20251202, 20251203, 20251204, 20251205);

    Optional<Double> correlation =
        computeStatisticsServiceImpl.computeCorrelation("v.to", "z.to", dates);
    assertTrue(correlation.isPresent());
    assertEquals(0.170723, correlation.get(), DELTA_PRECISION);
  }

  @Test
  void computeCorrelationMatrix_AllAssumptions() {
    when(tickerDataWarehouseService.getMarketData("z.to", 20251201)).thenReturn(10.20);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251202)).thenReturn(10.25);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251203)).thenReturn(10.10);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251204)).thenReturn(11.50);
    when(tickerDataWarehouseService.getMarketData("z.to", 20251205)).thenReturn(12.30);

    when(tickerDataWarehouseService.getMarketData("v.to", 20251201)).thenReturn(102.20);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251202)).thenReturn(104.25);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251203)).thenReturn(99.10);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251204)).thenReturn(96.99);
    when(tickerDataWarehouseService.getMarketData("v.to", 20251205)).thenReturn(101.55);

    when(tickerDataWarehouseService.getMarketData("o.to", 20251201)).thenReturn(10.0);
    when(tickerDataWarehouseService.getMarketData("o.to", 20251202)).thenReturn(12.0);
    when(tickerDataWarehouseService.getMarketData("o.to", 20251203)).thenReturn(11.0);
    when(tickerDataWarehouseService.getMarketData("o.to", 20251204)).thenReturn(13.0);
    when(tickerDataWarehouseService.getMarketData("o.to", 20251205)).thenReturn(15.0);

    when(tickerDataWarehouseService.getMarketData("a.to", 20251201)).thenReturn(20.0);
    when(tickerDataWarehouseService.getMarketData("a.to", 20251202)).thenReturn(21.0);
    when(tickerDataWarehouseService.getMarketData("a.to", 20251203)).thenReturn(23.0);
    when(tickerDataWarehouseService.getMarketData("a.to", 20251204)).thenReturn(22.0);
    when(tickerDataWarehouseService.getMarketData("a.to", 20251205)).thenReturn(24.0);

    List<Integer> dates = Lists.newArrayList(20251201, 20251202, 20251203, 20251204, 20251205);

    Optional<Table<String, String, Double>> correlationMatrix =
        computeStatisticsServiceImpl.computeCorrelationMatrix(
            Lists.newArrayList("v.to", "z.to", "o.to", "a.to"), dates);
    assertTrue(correlationMatrix.isPresent());
    assertEquals(16, correlationMatrix.get().size());
    System.out.println(correlationMatrix);

    // cross imnt pair correlation
    assertEquals(-.521633, correlationMatrix.get().get("o.to", "a.to"), DELTA_PRECISION);
    assertEquals(-.521633, correlationMatrix.get().get("a.to", "o.to"), DELTA_PRECISION);
    assertEquals(.170723, correlationMatrix.get().get("v.to", "z.to"), DELTA_PRECISION);
    assertEquals(.170723, correlationMatrix.get().get("z.to", "v.to"), DELTA_PRECISION);
    assertEquals(.714989, correlationMatrix.get().get("v.to", "o.to"), DELTA_PRECISION);
    assertEquals(.714989, correlationMatrix.get().get("o.to", "v.to"), DELTA_PRECISION);
    assertEquals(.198028, correlationMatrix.get().get("v.to", "a.to"), DELTA_PRECISION);
    assertEquals(.198028, correlationMatrix.get().get("a.to", "v.to"), DELTA_PRECISION);
    assertEquals(.583457, correlationMatrix.get().get("z.to", "o.to"), DELTA_PRECISION);
    assertEquals(.583457, correlationMatrix.get().get("o.to", "z.to"), DELTA_PRECISION);
    assertEquals(-.772773, correlationMatrix.get().get("z.to", "a.to"), DELTA_PRECISION);
    assertEquals(-.772773, correlationMatrix.get().get("a.to", "z.to"), DELTA_PRECISION);

    // diagonals - self imnt correlation
    assertEquals(1.0, correlationMatrix.get().get("v.to", "v.to"), DELTA_PRECISION);
    assertEquals(1.0, correlationMatrix.get().get("z.to", "z.to"), DELTA_PRECISION);
    assertEquals(1.0, correlationMatrix.get().get("o.to", "o.to"), DELTA_PRECISION);
    assertEquals(1.0, correlationMatrix.get().get("a.to", "a.to"), DELTA_PRECISION);
  }
}
