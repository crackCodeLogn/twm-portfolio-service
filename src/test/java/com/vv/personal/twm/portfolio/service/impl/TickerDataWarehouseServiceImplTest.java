package com.vv.personal.twm.portfolio.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Lists;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.TickerDataWarehouseConfig;
import com.vv.personal.twm.portfolio.model.market.OutdatedSymbol;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market.outdated.OutdatedSymbols;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.warehouse.market.TickerDataWarehouse;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
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
  public void testIdentifyMissingDatesDueToOutdated1() {
    OutdatedSymbols outdatedSymbols = new OutdatedSymbols();
    outdatedSymbols.loadFromFile("src/test/resources/OutdatedSymbols-tickerdw.txt");

    List<Integer> marketDates =
        Lists.newArrayList(
            20250530, 20250602, 20250603, 20250604, 20250605, 20250606, 20250609, 20250610,
            20250611, 20250612, 20250613, 20250616, 20250617, 20250618, 20250619, 20250620,
            20250623, 20250624, 20250625, 20250626, 20250627, 20250630);
    List<Pair<LocalDate, LocalDate>> resultPairs;

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250601, 20250626), marketDates);
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250602), DateFormatUtil.getLocalDate(20250623)),
        resultPairs.get(0));
  }

  @Test
  public void testIdentifyMissingDatesDueToOutdated2() {
    OutdatedSymbols outdatedSymbols = new OutdatedSymbols();
    outdatedSymbols.loadFromFile("src/test/resources/OutdatedSymbols-tickerdw2.txt");

    List<Integer> marketDates =
        Lists.newArrayList(
            20250530, 20250602, 20250603, 20250604, 20250605, 20250606, 20250609, 20250610,
            20250611, 20250612, 20250613, 20250616, 20250617, 20250618, 20250619, 20250620,
            20250623, 20250624, 20250625, 20250626, 20250627, 20250630, 20250701, 20250702,
            20250703, 20250704, 20250707, 20250708, 20250709, 20250710, 20250711, 20250714,
            20250715);
    List<Pair<LocalDate, LocalDate>> resultPairs;

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250601, 20250626), marketDates);
    assertEquals(2, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250603), DateFormatUtil.getLocalDate(20250610)),
        resultPairs.get(0));
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250619), DateFormatUtil.getLocalDate(20250620)),
        resultPairs.get(1));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250610, 20250618), marketDates);
    assertTrue(resultPairs.isEmpty());

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250610, 20250617), marketDates);
    assertTrue(resultPairs.isEmpty());

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250610, 20250619), marketDates);
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250619), DateFormatUtil.getLocalDate(20250620)),
        resultPairs.get(0));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250619, 20250620), marketDates);
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250619), DateFormatUtil.getLocalDate(20250620)),
        resultPairs.get(0));

    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250619, 20250701), marketDates);
    assertEquals(2, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250619), DateFormatUtil.getLocalDate(20250620)),
        resultPairs.get(0));
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250701), DateFormatUtil.getLocalDate(20250702)),
        resultPairs.get(1));

    // todo - investigate this further
    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbols, "^VIX", generateLocalDatePair(20250701, 20250708), marketDates);
    assertEquals(1, resultPairs.size());
    assertEquals(
        Pair.of(DateFormatUtil.getLocalDate(20250701), DateFormatUtil.getLocalDate(20250709)),
        resultPairs.get(0));
  }

  @Test
  public void testIdentifyMissingDatesDueToOutdated() {
    List<Pair<LocalDate, LocalDate>> resultPairs;
    OutdatedSymbol outdatedSymbol = new OutdatedSymbol(20250623, 20250629);

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

    outdatedSymbol = new OutdatedSymbol(20000101, 20211103);
    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20200623, 20211103));
    assertTrue(resultPairs.isEmpty());

    outdatedSymbol = new OutdatedSymbol(20000101, 20211103);
    resultPairs =
        tickerDataWarehouseServiceImpl.identifyMissingDatesDueToOutdated(
            outdatedSymbol, generateLocalDatePair(20200623, 20211103));
    assertTrue(resultPairs.isEmpty());
  }

  @Test
  public void testLocateIndexInMarketDates_Forward() {
    Optional<Integer> targetIndex;
    List<Integer> marketDates = Lists.newArrayList(20251229, 20251230, 20251231, 20260102);

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 29), marketDates, true);
    assertTrue(targetIndex.isPresent());
    assertEquals(0, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 28), marketDates, true);
    assertTrue(targetIndex.isPresent());
    assertEquals(0, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 27), marketDates, true);
    assertTrue(targetIndex.isPresent());
    assertEquals(0, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 30), marketDates, true);
    assertTrue(targetIndex.isPresent());
    assertEquals(1, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 31), marketDates, true);
    assertTrue(targetIndex.isPresent());
    assertEquals(2, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2026, 1, 1), marketDates, true);
    assertTrue(targetIndex.isPresent());
    assertEquals(3, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2026, 1, 2), marketDates, true);
    assertTrue(targetIndex.isPresent());
    assertEquals(3, targetIndex.get());

    // beyond the market dates in forward search mode
    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2026, 1, 3), marketDates, true);
    assertFalse(targetIndex.isPresent());

    // too far from market dates, out of the +- 10 days
    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 15), marketDates, true);
    assertFalse(targetIndex.isPresent());
  }

  @Test
  public void testLocateIndexInMarketDates_Backward() {
    Optional<Integer> targetIndex;
    List<Integer> marketDates = Lists.newArrayList(20251229, 20251230, 20251231, 20260102);

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 29), marketDates, false);
    assertTrue(targetIndex.isPresent());
    assertEquals(0, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 30), marketDates, false);
    assertTrue(targetIndex.isPresent());
    assertEquals(1, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 31), marketDates, false);
    assertTrue(targetIndex.isPresent());
    assertEquals(2, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2026, 1, 1), marketDates, false);
    assertTrue(targetIndex.isPresent());
    assertEquals(2, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2026, 1, 2), marketDates, false);
    assertTrue(targetIndex.isPresent());
    assertEquals(3, targetIndex.get());

    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2026, 1, 3), marketDates, false);
    assertTrue(targetIndex.isPresent());
    assertEquals(3, targetIndex.get());

    // too far from market dates, out of the +- 10 days
    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2026, 2, 15), marketDates, false);
    assertFalse(targetIndex.isPresent());

    // beyond the market dates in backward search mode
    targetIndex =
        tickerDataWarehouseServiceImpl.locateIndexInMarketDates(
            LocalDate.of(2025, 12, 28), marketDates, false);
    assertFalse(targetIndex.isPresent());
  }

  private Pair<LocalDate, LocalDate> generateLocalDatePair(int d1, int d2) {
    return Pair.of(DateFormatUtil.getLocalDate(d1), DateFormatUtil.getLocalDate(d2));
  }
}
