package com.vv.personal.twm.portfolio.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Lists;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.TickerDataWarehouseConfig;
import com.vv.personal.twm.portfolio.market.warehouse.TickerDataWarehouse;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import java.time.LocalDate;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Vivek
 * @since 2024-10-31
 */
@ExtendWith(MockitoExtension.class)
class TickerDataWarehouseServiceImplTest {

  @Mock private TickerDataWarehouseConfig tickerDataWarehouseConfig;
  @Mock private MarketDataPythonEngineFeign marketDataPythonEngineFeign;
  @Mock private MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;
  @Mock private TickerDataWarehouse tickerDataWarehouse;

  @InjectMocks private TickerDataWarehouseServiceImpl tickerDataWarehouseServiceImpl;

  @Test
  public void testIdentifyMissingDbDates() {
    List<Integer> dates =
        Lists.newArrayList(
            20241031, 20241101, 20241102, 20241103, 20241104, 20241105, 20241106, 20241107,
            20241108, 20241109, 20241110);
    MarketDataProto.Ticker ticker =
        MarketDataProto.Ticker.newBuilder()
            .setSymbol("test-v2.to")
            .addData(MarketDataProto.Value.newBuilder().setDate(20241103).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241104).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241107).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241109).build())
            .build();

    List<Pair<LocalDate, LocalDate>> missingDbDates =
        tickerDataWarehouseServiceImpl.identifyMissingDbDates(ticker, dates);
    System.out.println(missingDbDates);
    assertEquals(4, missingDbDates.size());
    assertEquals(
        Pair.of(LocalDate.of(2024, 10, 31), LocalDate.of(2024, 11, 3)), missingDbDates.get(0));
    assertEquals(
        Pair.of(LocalDate.of(2024, 11, 5), LocalDate.of(2024, 11, 7)), missingDbDates.get(1));
    assertEquals(
        Pair.of(LocalDate.of(2024, 11, 8), LocalDate.of(2024, 11, 9)), missingDbDates.get(2));
    assertEquals(
        Pair.of(LocalDate.of(2024, 11, 10), LocalDate.of(2024, 11, 11)), missingDbDates.get(3));
  }

  @Test
  public void testIdentifyMissingDbDates2() {
    List<Integer> dates =
        Lists.newArrayList(
            20241031, 20241101, 20241102, 20241103, 20241104, 20241105, 20241106, 20241107,
            20241108, 20241109, 20241110);
    MarketDataProto.Ticker ticker =
        MarketDataProto.Ticker.newBuilder()
            .setSymbol("test-v2.to")
            .addData(MarketDataProto.Value.newBuilder().setDate(20241103).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241104).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241107).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241109).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241110).build())
            .build();

    List<Pair<LocalDate, LocalDate>> missingDbDates =
        tickerDataWarehouseServiceImpl.identifyMissingDbDates(ticker, dates);
    System.out.println(missingDbDates);
    assertEquals(3, missingDbDates.size());
    assertEquals(
        Pair.of(LocalDate.of(2024, 10, 31), LocalDate.of(2024, 11, 3)), missingDbDates.get(0));
    assertEquals(
        Pair.of(LocalDate.of(2024, 11, 5), LocalDate.of(2024, 11, 7)), missingDbDates.get(1));
    assertEquals(
        Pair.of(LocalDate.of(2024, 11, 8), LocalDate.of(2024, 11, 9)), missingDbDates.get(2));
  }

  @Test
  public void testIdentifyMissingDbDates_NoMissingDates() {
    List<Integer> dates = Lists.newArrayList(20241101, 20241102, 20241103, 20241104);
    MarketDataProto.Ticker ticker =
        MarketDataProto.Ticker.newBuilder()
            .setSymbol("test-v2.to")
            .addData(MarketDataProto.Value.newBuilder().setDate(20241103).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241104).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241102).build())
            .addData(MarketDataProto.Value.newBuilder().setDate(20241101).build())
            .build();

    List<Pair<LocalDate, LocalDate>> missingDbDates =
        tickerDataWarehouseServiceImpl.identifyMissingDbDates(ticker, dates);
    System.out.println(missingDbDates);
    assertTrue(missingDbDates.isEmpty());
  }

  @Test
  public void testIdentifyMissingDbDates_AllMissingDates() {
    List<Integer> dates = Lists.newArrayList(20241101, 20241102, 20241103, 20241104);
    MarketDataProto.Ticker ticker =
        MarketDataProto.Ticker.newBuilder().setSymbol("test-v2.to").build();

    List<Pair<LocalDate, LocalDate>> missingDbDates =
        tickerDataWarehouseServiceImpl.identifyMissingDbDates(ticker, dates);
    System.out.println(missingDbDates);
    assertFalse(missingDbDates.isEmpty());
    assertEquals(1, missingDbDates.size());
    assertEquals("2024-11-01", missingDbDates.get(0).getLeft().toString());
    assertEquals("2024-11-05", missingDbDates.get(0).getRight().toString());
  }

  @Test
  public void testConvertDate() {
    LocalDate result = tickerDataWarehouseServiceImpl.convertDate(20241031);
    assertEquals(result, LocalDate.of(2024, 10, 31));
  }
}
