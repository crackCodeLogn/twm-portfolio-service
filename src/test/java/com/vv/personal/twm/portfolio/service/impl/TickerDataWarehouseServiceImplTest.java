package com.vv.personal.twm.portfolio.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Lists;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.TickerDataWarehouseConfig;
import com.vv.personal.twm.portfolio.model.market.OutdatedSymbol;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.warehouse.market.TickerDataWarehouse;
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

  @Test
  public void testIdentifyMissingDatesDueToOutdated() {
    List<Pair<LocalDate, LocalDate>> resultPairs;
    OutdatedSymbol outdatedSymbol = new OutdatedSymbol("test-v2.to", 20250623, 20250629);

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250601, 20250626));
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250601), DateFormatUtil.getLocalDate(20250623)),
        resultPairs.get(0));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250601, 20250623));
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250601), DateFormatUtil.getLocalDate(20250623)),
        resultPairs.get(0));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250629, 20251001)); // note start date increase
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250630), DateFormatUtil.getLocalDate(20251001)),
        resultPairs.get(0));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250601, 20250620));
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250601), DateFormatUtil.getLocalDate(20250620)),
        resultPairs.get(0));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250701, 20250726));
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250701), DateFormatUtil.getLocalDate(20250726)),
        resultPairs.get(0));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250623, 20250629));
    assertTrue(resultPairs.isEmpty());

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250625, 20250627));
    assertTrue(resultPairs.isEmpty());

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20250601, 20250715));
    assertEquals(2, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250601), DateFormatUtil.getLocalDate(20250623)),
        resultPairs.get(0));
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250630), DateFormatUtil.getLocalDate(20250715)),
        resultPairs.get(1)); // note start date increase

    outdatedSymbol = new OutdatedSymbol("test-v2.to", 20000101, 20211103);
    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20200623, 20211103));
    assertTrue(resultPairs.isEmpty());

    outdatedSymbol = new OutdatedSymbol("test-v2.to", 20000101, 20211103);
    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20200623, 20211103));
    assertTrue(resultPairs.isEmpty());
  }

  private Pair<LocalDate, LocalDate> generateLocalDatePair(int d1, int d2) {
    return Pair.of(DateFormatUtil.getLocalDate(d1), DateFormatUtil.getLocalDate(d2));
  }
}
