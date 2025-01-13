package com.vv.personal.twm.portfolio.service;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.SELL;
import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.model.market.DataList;
import com.vv.personal.twm.portfolio.model.market.DividendRecord;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.util.TestInstrument;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Vivek
 * @since 2024-09-13
 */
@ExtendWith(MockitoExtension.class)
class CompleteMarketDataServiceTest {

  @Mock private TickerDataWarehouseService tickerDataWarehouseService;

  private CompleteMarketDataService completeMarketDataService;

  @BeforeEach
  void setUp() {
    completeMarketDataService = new CompleteMarketDataService(new DateLocalDateCache());
  }

  @Test
  public void populateMap() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder().addAllInstruments(generateTestInstruments()).build();
    System.out.println(portfolio);

    completeMarketDataService.populate(portfolio);
    completeMarketDataService.computeAcb();
    Map<String, Map<MarketDataProto.AccountType, DataList>> result =
        completeMarketDataService.getMarketData();

    assertFalse(result.isEmpty());
    assertTrue(result.containsKey("CM.TO"));
    DataList cibcTfsaList = result.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertNotNull(cibcTfsaList);
    assertEquals(3, cibcTfsaList.getBlocks());
    assertEquals(50.1, cibcTfsaList.getHead().getAcb().getTotalAcb());
    assertEquals(0, cibcTfsaList.getHead().getNext().getAcb().getTotalAcb());
    assertEquals(150, cibcTfsaList.getTail().getAcb().getTotalAcb());

    DataList cmNrList = result.get("CM.TO").get(MarketDataProto.AccountType.NR);
    assertNotNull(cmNrList);
    assertEquals(1, cmNrList.getBlocks());
    assertEquals(150, cmNrList.getHead().getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(15, cmNrList.getHead().getAcb().getAcbPerUnit(), DELTA_PRECISION);

    assertTrue(result.containsKey("BNS.TO"));
    DataList bnsTfsaList = result.get("BNS.TO").get(MarketDataProto.AccountType.TFSA);
    assertNotNull(bnsTfsaList);
    assertEquals(1, bnsTfsaList.getBlocks());
    assertEquals(100, bnsTfsaList.getHead().getAcb().getTotalAcb(), DELTA_PRECISION);
    assertEquals(5, bnsTfsaList.getHead().getAcb().getAcbPerUnit(), DELTA_PRECISION);
  }

  @Test
  public void testComputePnLWithNoDividends() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder()
            .addAllInstruments(generateTestInstruments3())
            .build();
    System.out.println(portfolio);
    completeMarketDataService.populate(portfolio);
    completeMarketDataService
        .computeAcb(); // at this point, assumption is that acb compute is accurate

    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240909)).thenReturn(5.01);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240910)).thenReturn(5.3);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240911)).thenReturn(5.5);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240912)).thenReturn(7.0);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240913)).thenReturn(9.0);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240916)).thenReturn(9.0);
    when(tickerDataWarehouseService.getDates())
        .thenReturn(
            Lists.newArrayList(
                DateFormatUtil.getLocalDate(20240905),
                DateFormatUtil.getLocalDate(20240906),
                DateFormatUtil.getLocalDate(20240909),
                DateFormatUtil.getLocalDate(20240910),
                DateFormatUtil.getLocalDate(20240911),
                DateFormatUtil.getLocalDate(20240912),
                DateFormatUtil.getLocalDate(20240913),
                DateFormatUtil.getLocalDate(20240916)));

    completeMarketDataService.setTickerDataWarehouseService(tickerDataWarehouseService);
    completeMarketDataService.computePnL();
    Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedPnLMap =
        completeMarketDataService.getUnrealizedDatePnLMap();
    System.out.println("unrealizedPnLMap => " + unrealizedPnLMap);
    assertFalse(unrealizedPnLMap.isEmpty());
    assertFalse(unrealizedPnLMap.containsKey(20240905));
    assertFalse(unrealizedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, unrealizedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, unrealizedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, unrealizedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        74.6,
        unrealizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION); // should be 74.61 though
    assertEquals(
        104.6,
        unrealizedPnLMap.get(20240913).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        104.6,
        unrealizedPnLMap.get(20240916).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);

    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> unrealizedImntPnLMap =
        completeMarketDataService.getUnrealizedImntPnLMap();
    System.out.println("unrealizedImntPnLMap = " + unrealizedImntPnLMap);
    assertFalse(unrealizedImntPnLMap.isEmpty());
    assertTrue(unrealizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> unrealizedImntPnLCibcMap =
        unrealizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(0.0, unrealizedImntPnLCibcMap.get(20240909), DELTA_PRECISION);
    assertEquals(5.6, unrealizedImntPnLCibcMap.get(20240910), DELTA_PRECISION);
    assertEquals(9.6, unrealizedImntPnLCibcMap.get(20240911), DELTA_PRECISION);
    assertEquals(74.6, unrealizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);
    assertEquals(104.6, unrealizedImntPnLCibcMap.get(20240913), DELTA_PRECISION);
    assertEquals(104.6, unrealizedImntPnLCibcMap.get(20240916), DELTA_PRECISION);

    Map<Integer, Map<MarketDataProto.AccountType, Double>> realizedPnLMap =
        completeMarketDataService.getRealizedDatePnLMap();
    System.out.println("realizedPnLMap => " + realizedPnLMap);
    assertFalse(realizedPnLMap.isEmpty());
    assertFalse(realizedPnLMap.containsKey(20240905));
    assertFalse(realizedPnLMap.containsKey(20240906));
    assertFalse(realizedPnLMap.containsKey(20240909));
    assertFalse(realizedPnLMap.containsKey(20240910));
    assertNull(realizedPnLMap.get(20240911));
    assertEquals(
        9.9, realizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(realizedPnLMap.containsKey(20240913));
    assertFalse(realizedPnLMap.containsKey(20240916));

    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> realizedImntPnLMap =
        completeMarketDataService.getRealizedImntPnLMap();
    System.out.println("realizedImntPnLMap = " + realizedImntPnLMap);
    assertFalse(realizedImntPnLMap.isEmpty());
    assertTrue(realizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> realizedImntPnLCibcMap =
        realizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(9.9, realizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);

    Map<Integer, Map<MarketDataProto.AccountType, Double>> combinedPnLMap =
        completeMarketDataService.getCombinedDatePnLMap();
    System.out.println("combinedPnLMap => " + combinedPnLMap);
    assertFalse(combinedPnLMap.isEmpty());
    assertFalse(combinedPnLMap.containsKey(20240905));
    assertFalse(combinedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, combinedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, combinedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, combinedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        84.5, combinedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        104.6, combinedPnLMap.get(20240913).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        104.6, combinedPnLMap.get(20240916).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
  }

  @Test
  public void testComputePnLWithNoDividendsAndLastTransactionAsFullSellAndFurtherDates() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder()
            .addAllInstruments(generateTestInstruments4WithLastBlockAsFullSellOff())
            .build();
    System.out.println(portfolio);
    completeMarketDataService.populate(portfolio);
    completeMarketDataService
        .computeAcb(); // at this point, assumption is that acb compute is accurate

    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240909)).thenReturn(5.01);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240910)).thenReturn(5.3);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240911)).thenReturn(5.5);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240912)).thenReturn(7.0);
    when(tickerDataWarehouseService.getDates())
        .thenReturn(
            Lists.newArrayList(
                DateFormatUtil.getLocalDate(20240905),
                DateFormatUtil.getLocalDate(20240906),
                DateFormatUtil.getLocalDate(20240909),
                DateFormatUtil.getLocalDate(20240910),
                DateFormatUtil.getLocalDate(20240911),
                DateFormatUtil.getLocalDate(20240912),
                DateFormatUtil.getLocalDate(20240913),
                DateFormatUtil.getLocalDate(20240916)));

    completeMarketDataService.setTickerDataWarehouseService(tickerDataWarehouseService);
    completeMarketDataService.computePnL();
    Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedPnLMap =
        completeMarketDataService.getUnrealizedDatePnLMap();
    System.out.println("unrealizedPnLMap => " + unrealizedPnLMap);
    assertFalse(unrealizedPnLMap.isEmpty());
    assertFalse(unrealizedPnLMap.containsKey(20240905));
    assertFalse(unrealizedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, unrealizedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, unrealizedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, unrealizedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        0, unrealizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    // one of the changed part
    assertFalse(unrealizedPnLMap.containsKey(20240913));
    assertFalse(unrealizedPnLMap.containsKey(20240916));

    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> unrealizedImntPnLMap =
        completeMarketDataService.getUnrealizedImntPnLMap();
    System.out.println("unrealizedImntPnLMap = " + unrealizedImntPnLMap);
    assertFalse(unrealizedImntPnLMap.isEmpty());
    assertTrue(unrealizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> unrealizedImntPnLCibcMap =
        unrealizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(0.0, unrealizedImntPnLCibcMap.get(20240909), DELTA_PRECISION);
    assertEquals(5.6, unrealizedImntPnLCibcMap.get(20240910), DELTA_PRECISION);
    assertEquals(9.6, unrealizedImntPnLCibcMap.get(20240911), DELTA_PRECISION);
    assertEquals(0, unrealizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);
    assertNull(unrealizedImntPnLCibcMap.get(20240913));
    assertNull(unrealizedImntPnLCibcMap.get(20240916));

    Map<Integer, Map<MarketDataProto.AccountType, Double>> realizedPnLMap =
        completeMarketDataService.getRealizedDatePnLMap();
    System.out.println("realizedPnLMap => " + realizedPnLMap);
    assertFalse(realizedPnLMap.isEmpty());
    assertFalse(realizedPnLMap.containsKey(20240905));
    assertFalse(realizedPnLMap.containsKey(20240906));
    assertFalse(realizedPnLMap.containsKey(20240909));
    assertFalse(realizedPnLMap.containsKey(20240910));
    assertNull(realizedPnLMap.get(20240911));
    assertEquals(
        39.6, realizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(realizedPnLMap.containsKey(20240913));
    assertFalse(realizedPnLMap.containsKey(20240916));

    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> realizedImntPnLMap =
        completeMarketDataService.getRealizedImntPnLMap();
    System.out.println("realizedImntPnLMap = " + realizedImntPnLMap);
    assertFalse(realizedImntPnLMap.isEmpty());
    assertTrue(realizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> realizedImntPnLCibcMap =
        realizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(39.6, realizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);

    Map<Integer, Map<MarketDataProto.AccountType, Double>> combinedPnLMap =
        completeMarketDataService.getCombinedDatePnLMap();
    System.out.println("combinedPnLMap => " + combinedPnLMap);
    assertFalse(combinedPnLMap.isEmpty());
    assertFalse(combinedPnLMap.containsKey(20240905));
    assertFalse(combinedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, combinedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, combinedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, combinedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        39.6, combinedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(combinedPnLMap.containsKey(20240913));
    assertFalse(combinedPnLMap.containsKey(20240916));
  }

  @Test
  public void
      testComputePnLWithNoDividendsAndIntermediateSellAndLastTransactionAsFullSellAndFurtherDates() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder()
            .addAllInstruments(
                generateTestInstruments5WithIntermediateSellAndLastBlockAsFullSellOff())
            .build();
    System.out.println(portfolio);
    completeMarketDataService.populate(portfolio);
    completeMarketDataService
        .computeAcb(); // at this point, assumption is that acb compute is accurate

    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240909)).thenReturn(5.01);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240910)).thenReturn(5.3);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240911)).thenReturn(5.5);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240912)).thenReturn(14.0);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240913)).thenReturn(6.5);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240916)).thenReturn(9.0);
    // when(tickerDataWarehouseService.getMarketData("CM.TO", 20240917)).thenReturn(9.5);
    when(tickerDataWarehouseService.getDates())
        .thenReturn(
            Lists.newArrayList(
                DateFormatUtil.getLocalDate(20240905),
                DateFormatUtil.getLocalDate(20240906),
                DateFormatUtil.getLocalDate(20240909),
                DateFormatUtil.getLocalDate(20240910),
                DateFormatUtil.getLocalDate(20240911),
                DateFormatUtil.getLocalDate(20240912),
                DateFormatUtil.getLocalDate(20240913),
                DateFormatUtil.getLocalDate(20240916),
                DateFormatUtil.getLocalDate(20240917)));

    completeMarketDataService.setTickerDataWarehouseService(tickerDataWarehouseService);
    completeMarketDataService.computePnL();
    Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedPnLMap =
        completeMarketDataService.getUnrealizedDatePnLMap();
    System.out.println("unrealizedPnLMap => " + unrealizedPnLMap);
    assertFalse(unrealizedPnLMap.isEmpty());
    assertFalse(unrealizedPnLMap.containsKey(20240905));
    assertFalse(unrealizedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, unrealizedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, unrealizedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, unrealizedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        179.6, // should be 179.7 tbh, but fine
        unrealizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        67.1,
        unrealizedPnLMap.get(20240913).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        0, unrealizedPnLMap.get(20240916).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(unrealizedPnLMap.containsKey(20240917));

    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> unrealizedImntPnLMap =
        completeMarketDataService.getUnrealizedImntPnLMap();
    System.out.println("unrealizedImntPnLMap = " + unrealizedImntPnLMap);
    assertFalse(unrealizedImntPnLMap.isEmpty());
    assertTrue(unrealizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> unrealizedImntPnLCibcMap =
        unrealizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(0.0, unrealizedImntPnLCibcMap.get(20240909), DELTA_PRECISION);
    assertEquals(5.6, unrealizedImntPnLCibcMap.get(20240910), DELTA_PRECISION);
    assertEquals(9.6, unrealizedImntPnLCibcMap.get(20240911), DELTA_PRECISION);
    assertEquals(179.6, unrealizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);
    assertEquals(67.1, unrealizedImntPnLCibcMap.get(20240913), DELTA_PRECISION);
    assertEquals(0.0, unrealizedImntPnLCibcMap.get(20240916), DELTA_PRECISION);
    assertNull(unrealizedImntPnLCibcMap.get(20240917));

    Map<Integer, Map<MarketDataProto.AccountType, Double>> realizedPnLMap =
        completeMarketDataService.getRealizedDatePnLMap();
    System.out.println("realizedPnLMap => " + realizedPnLMap);
    assertFalse(realizedPnLMap.isEmpty());
    assertFalse(realizedPnLMap.containsKey(20240905));
    assertFalse(realizedPnLMap.containsKey(20240906));
    assertFalse(realizedPnLMap.containsKey(20240909));
    assertFalse(realizedPnLMap.containsKey(20240910));
    assertNull(realizedPnLMap.get(20240911));
    assertEquals(
        44.9, realizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(realizedPnLMap.containsKey(20240913));
    assertEquals(
        129.6, realizedPnLMap.get(20240916).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(realizedPnLMap.containsKey(20240917));

    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> realizedImntPnLMap =
        completeMarketDataService.getRealizedImntPnLMap();
    System.out.println("realizedImntPnLMap = " + realizedImntPnLMap);
    assertFalse(realizedImntPnLMap.isEmpty());
    assertTrue(realizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> realizedImntPnLCibcMap =
        realizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(2, realizedImntPnLCibcMap.size());
    assertEquals(44.9, realizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);
    assertEquals(129.6, realizedImntPnLCibcMap.get(20240916), DELTA_PRECISION);

    Map<Integer, Map<MarketDataProto.AccountType, Double>> combinedPnLMap =
        completeMarketDataService.getCombinedDatePnLMap();
    System.out.println("combinedPnLMap => " + combinedPnLMap);
    assertFalse(combinedPnLMap.isEmpty());
    assertFalse(combinedPnLMap.containsKey(20240905));
    assertFalse(combinedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, combinedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, combinedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, combinedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        224.5, combinedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        67.1, combinedPnLMap.get(20240913).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        129.6, combinedPnLMap.get(20240916).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(combinedPnLMap.containsKey(20240917));
  }

  @Test
  public void testComputePnLWithDividends() {
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240909)).thenReturn(5.01);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240910)).thenReturn(5.3);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240911)).thenReturn(5.5);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240912)).thenReturn(7.0);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240913)).thenReturn(9.0);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240914)).thenReturn(null);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240916)).thenReturn(9.0);
    when(tickerDataWarehouseService.getDates())
        .thenReturn(
            Lists.newArrayList(
                DateFormatUtil.getLocalDate(20240905),
                DateFormatUtil.getLocalDate(20240906),
                DateFormatUtil.getLocalDate(20240909),
                DateFormatUtil.getLocalDate(20240910),
                DateFormatUtil.getLocalDate(20240911),
                DateFormatUtil.getLocalDate(20240912),
                DateFormatUtil.getLocalDate(20240913),
                DateFormatUtil.getLocalDate(20240916)));

    completeMarketDataService.setTickerDataWarehouseService(tickerDataWarehouseService);
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder()
            .addAllInstruments(generateTestInstruments3())
            .build();
    System.out.println(portfolio);
    completeMarketDataService.populate(portfolio);
    completeMarketDataService
        .computeAcb(); // at this point, assumption is that acb compute is accurate
    completeMarketDataService.populateDividends(
        MarketDataProto.Portfolio.newBuilder()
            .addAllInstruments(generateTestDividendInstruments())
            .build());
    completeMarketDataService.computePnL();

    Map<String, Map<MarketDataProto.AccountType, Map<Integer, DividendRecord>>> dividendsMap =
        completeMarketDataService.getImntDividendsMap();
    assertEquals(1, dividendsMap.size());
    assertEquals(
        10.0,
        dividendsMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA).get(20240912).dividend(),
        DELTA_PRECISION);
    assertEquals(
        "DIVIDEND_20240912-TFSA-0-CM.TO",
        dividendsMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA).get(20240912).orderId());
    assertEquals(
        20.0,
        dividendsMap.get("CM.TO").get(MarketDataProto.AccountType.NR).get(20240912).dividend(),
        DELTA_PRECISION);
    assertEquals(
        5.0,
        dividendsMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA).get(20240914).dividend(),
        DELTA_PRECISION);
    assertEquals(2, dividendsMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA).size());
    assertEquals(1, dividendsMap.get("CM.TO").get(MarketDataProto.AccountType.NR).size());

    TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> dateDividendsCumulativeMap =
        completeMarketDataService.getDateDividendsCumulativeMap();
    System.out.println("dateDividendsCumulativeMap => " + dateDividendsCumulativeMap);
    assertEquals(3, dateDividendsCumulativeMap.size());
    assertEquals(
        0.0,
        dateDividendsCumulativeMap.get(0).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        0.0,
        dateDividendsCumulativeMap.get(0).get(MarketDataProto.AccountType.NR),
        DELTA_PRECISION);
    assertEquals(
        0.0,
        dateDividendsCumulativeMap.get(0).get(MarketDataProto.AccountType.IND),
        DELTA_PRECISION);
    assertEquals(
        10.0,
        dateDividendsCumulativeMap.get(20240912).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        20.0,
        dateDividendsCumulativeMap.get(20240912).get(MarketDataProto.AccountType.NR),
        DELTA_PRECISION);
    assertEquals(
        0.0,
        dateDividendsCumulativeMap.get(20240912).get(MarketDataProto.AccountType.IND),
        DELTA_PRECISION);
    assertEquals(
        15.0,
        dateDividendsCumulativeMap.get(20240914).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        20.0,
        dateDividendsCumulativeMap.get(20240914).get(MarketDataProto.AccountType.NR),
        DELTA_PRECISION);
    assertEquals(
        0.0,
        dateDividendsCumulativeMap.get(20240914).get(MarketDataProto.AccountType.IND),
        DELTA_PRECISION);

    Map<Integer, Map<MarketDataProto.AccountType, Double>> dividendsDateDivMap =
        completeMarketDataService.getDateDividendsMap();
    System.out.println("dividendsDateDivMap => " + dividendsDateDivMap);
    assertFalse(dividendsDateDivMap.isEmpty());
    assertFalse(dividendsDateDivMap.containsKey(20240905));
    assertFalse(dividendsDateDivMap.containsKey(20240906));
    assertFalse(dividendsDateDivMap.containsKey(20240909));
    assertFalse(dividendsDateDivMap.containsKey(20240910));
    assertFalse(dividendsDateDivMap.containsKey(20240911));
    assertEquals(
        10.0,
        dividendsDateDivMap.get(20240912).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        20.0,
        dividendsDateDivMap.get(20240912).get(MarketDataProto.AccountType.NR),
        DELTA_PRECISION);
    assertFalse(dividendsDateDivMap.containsKey(20240913));
    assertEquals(
        5.0,
        dividendsDateDivMap.get(20240914).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertNull(dividendsDateDivMap.get(20240914).get(MarketDataProto.AccountType.NR));
    assertFalse(dividendsDateDivMap.containsKey(20240916));

    Map<String, Map<MarketDataProto.AccountType, TreeMap<Integer, Double>>>
        realizedImntWithDividendPnLMap =
            completeMarketDataService.getRealizedImntWithDividendPnLMap(); // cumulative
    System.out.println("realizedImntWithDividendPnLMap => " + realizedImntWithDividendPnLMap);
    assertFalse(realizedImntWithDividendPnLMap.isEmpty());
    assertEquals(1, realizedImntWithDividendPnLMap.size());
    assertTrue(realizedImntWithDividendPnLMap.containsKey("CM.TO"));
    Map<MarketDataProto.AccountType, TreeMap<Integer, Double>> typeMapMap =
        realizedImntWithDividendPnLMap.get("CM.TO");
    assertEquals(2, typeMapMap.size());
    TreeMap<Integer, Double> datePriceTfsaMap = typeMapMap.get(MarketDataProto.AccountType.TFSA);
    TreeMap<Integer, Double> datePriceNrMap = typeMapMap.get(MarketDataProto.AccountType.NR);
    assertEquals(0, datePriceTfsaMap.floorEntry(20240905).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceNrMap.floorEntry(20240905).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceTfsaMap.floorEntry(20240906).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceNrMap.floorEntry(20240906).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceTfsaMap.floorEntry(20240909).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceNrMap.floorEntry(20240909).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceTfsaMap.floorEntry(20240910).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceNrMap.floorEntry(20240910).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceTfsaMap.floorEntry(20240911).getValue(), DELTA_PRECISION);
    assertEquals(0, datePriceNrMap.floorEntry(20240911).getValue(), DELTA_PRECISION);
    assertEquals(19.9, datePriceTfsaMap.floorEntry(20240912).getValue(), DELTA_PRECISION);
    assertEquals(20, datePriceNrMap.floorEntry(20240912).getValue(), DELTA_PRECISION);
    assertEquals(19.9, datePriceTfsaMap.floorEntry(20240913).getValue(), DELTA_PRECISION);
    assertEquals(20, datePriceNrMap.floorEntry(20240913).getValue(), DELTA_PRECISION);
    assertEquals(24.9, datePriceTfsaMap.floorEntry(20240914).getValue(), DELTA_PRECISION);
    assertEquals(20.0, datePriceNrMap.floorEntry(20240914).getValue(), DELTA_PRECISION);
    assertEquals(24.9, datePriceTfsaMap.floorEntry(20240915).getValue(), DELTA_PRECISION);
    assertEquals(20, datePriceNrMap.floorEntry(20240915).getValue(), DELTA_PRECISION);
    assertEquals(24.9, datePriceTfsaMap.floorEntry(20240916).getValue(), DELTA_PRECISION);
    assertEquals(20.0, datePriceNrMap.floorEntry(20240916).getValue(), DELTA_PRECISION);

    TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> realizedWithDividendDatePnLMap =
        completeMarketDataService.getRealizedWithDividendDatePnLMap();
    System.out.println("realizedWithDividendDatePnLMap => " + realizedWithDividendDatePnLMap);
    assertEquals(10, realizedWithDividendDatePnLMap.size());
    Map<MarketDataProto.AccountType, Double> typeDividendMap;
    typeDividendMap = realizedWithDividendDatePnLMap.get(0);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240905);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240906);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240909);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240910);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240911);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240912);
    assertEquals(19.9, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(20, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240913);
    assertEquals(19.9, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(20.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240914);
    assertEquals(24.9, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(20, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    assertFalse(realizedWithDividendDatePnLMap.containsKey(20240915));
    typeDividendMap = realizedWithDividendDatePnLMap.get(20240916);
    assertEquals(24.9, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(20, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);

    // todo - work
    TreeMap<Integer, Map<MarketDataProto.AccountType, Double>> combinedDatePnLCumulativeMap =
        completeMarketDataService.getCombinedDatePnLCumulativeMap();
    System.out.println("combinedDatePnLCumulativeMap => " + combinedDatePnLCumulativeMap);
    assertFalse(combinedDatePnLCumulativeMap.isEmpty());
    typeDividendMap = combinedDatePnLCumulativeMap.get(0);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240905).getValue();
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240906).getValue();
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240909).getValue();
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240910).getValue();
    assertEquals(5.6, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240911).getValue();
    assertEquals(9.6, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(0.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240912).getValue();
    assertEquals(
        94.5, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION); // 19.9+74.6
    assertEquals(20, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240913).getValue();
    assertEquals(
        124.5,
        typeDividendMap.get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION); // 104.6+19.9
    assertEquals(20.0, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240914).getValue();
    // this is special situation. 20240914 is not present in combinedDatePnLMap as it got alive
    // due to div payout on a non-market date. Thus, the 14th will not exist, but its calc will show
    // up on next date
    assertEquals(
        124.5,
        typeDividendMap.get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION); // 104.6+24.9
    assertEquals(20, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    assertFalse(combinedDatePnLCumulativeMap.containsKey(20240915));
    typeDividendMap = combinedDatePnLCumulativeMap.floorEntry(20240916).getValue();
    // the 14th tfsa div of $5 shows up here
    assertEquals(129.5, typeDividendMap.get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(20, typeDividendMap.get(MarketDataProto.AccountType.NR), DELTA_PRECISION);

    // SECTION START - SANITY checks on the non dividend impacted data structures
    // unrealized calc should not be impacted due to divs, thus this is just for sanity
    Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedPnLMap =
        completeMarketDataService.getUnrealizedDatePnLMap();
    System.out.println("unrealizedPnLMap => " + unrealizedPnLMap);
    assertFalse(unrealizedPnLMap.isEmpty());
    assertFalse(unrealizedPnLMap.containsKey(20240905));
    assertFalse(unrealizedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, unrealizedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, unrealizedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, unrealizedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        74.6,
        unrealizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION); // should be 74.61 though
    assertEquals(
        104.6,
        unrealizedPnLMap.get(20240913).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);
    assertEquals(
        104.6,
        unrealizedPnLMap.get(20240916).get(MarketDataProto.AccountType.TFSA),
        DELTA_PRECISION);

    // unrealized calc should not be impacted due to divs, thus this is just for sanity
    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> unrealizedImntPnLMap =
        completeMarketDataService.getUnrealizedImntPnLMap();
    System.out.println("unrealizedImntPnLMap = " + unrealizedImntPnLMap);
    assertFalse(unrealizedImntPnLMap.isEmpty());
    assertTrue(unrealizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> unrealizedImntPnLCibcMap =
        unrealizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(0.0, unrealizedImntPnLCibcMap.get(20240909), DELTA_PRECISION);
    assertEquals(5.6, unrealizedImntPnLCibcMap.get(20240910), DELTA_PRECISION);
    assertEquals(9.6, unrealizedImntPnLCibcMap.get(20240911), DELTA_PRECISION);
    assertEquals(74.6, unrealizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);
    assertEquals(104.6, unrealizedImntPnLCibcMap.get(20240913), DELTA_PRECISION);
    assertEquals(104.6, unrealizedImntPnLCibcMap.get(20240916), DELTA_PRECISION);

    // pure realized calc should not be impacted due to divs, thus this is just for sanity
    Map<Integer, Map<MarketDataProto.AccountType, Double>> realizedPnLMap =
        completeMarketDataService.getRealizedDatePnLMap();
    System.out.println("realizedPnLMap => " + realizedPnLMap);
    assertFalse(realizedPnLMap.isEmpty());
    assertFalse(realizedPnLMap.containsKey(20240905));
    assertFalse(realizedPnLMap.containsKey(20240906));
    assertFalse(realizedPnLMap.containsKey(20240909));
    assertFalse(realizedPnLMap.containsKey(20240910));
    assertNull(realizedPnLMap.get(20240911));
    assertEquals(
        9.9, realizedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        0, realizedPnLMap.get(20240912).get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    assertFalse(realizedPnLMap.containsKey(20240913));
    assertFalse(realizedPnLMap.containsKey(20240914));
    assertFalse(realizedPnLMap.containsKey(20240915));
    assertFalse(realizedPnLMap.containsKey(20240916));

    // pure realized calc should not be impacted due to divs, thus this is just for sanity
    Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>> realizedImntPnLMap =
        completeMarketDataService.getRealizedImntPnLMap();
    System.out.println("realizedImntPnLMap = " + realizedImntPnLMap);
    assertFalse(realizedImntPnLMap.isEmpty());
    assertTrue(realizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> realizedImntPnLCibcMap =
        realizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(9.9, realizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);

    // pure combined calc should not be impacted due to divs, thus this is just for sanity
    Map<Integer, Map<MarketDataProto.AccountType, Double>> combinedPnLMap =
        completeMarketDataService.getCombinedDatePnLMap();
    System.out.println("combinedPnLMap => " + combinedPnLMap);
    assertFalse(combinedPnLMap.isEmpty());
    assertFalse(combinedPnLMap.containsKey(20240905));
    assertFalse(combinedPnLMap.containsKey(20240906));
    assertEquals(
        0.0, combinedPnLMap.get(20240909).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        5.6, combinedPnLMap.get(20240910).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        9.6, combinedPnLMap.get(20240911).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        84.5, combinedPnLMap.get(20240912).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertEquals(
        0.0, combinedPnLMap.get(20240912).get(MarketDataProto.AccountType.NR), DELTA_PRECISION);
    assertEquals(
        104.6, combinedPnLMap.get(20240913).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    assertFalse(combinedPnLMap.containsKey(20240914));
    assertFalse(combinedPnLMap.containsKey(20240915));
    assertEquals(
        104.6, combinedPnLMap.get(20240916).get(MarketDataProto.AccountType.TFSA), DELTA_PRECISION);
    // SECTION END - SANITY checks
  }

  private List<MarketDataProto.Instrument> generateTestInstruments() {
    return Lists.newArrayList(
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.1)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("bns.to")
            .name("bank of nova scotia")
            .qty(20)
            .price(100)
            .date(20240912)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(150)
            .date(20240920)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(5)
            .price(120)
            .direction(SELL)
            .date(20240913) // forced fudging
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(150)
            .date(20240920)
            .accountType(MarketDataProto.AccountType.NR)
            .build()
            .getInstrument());
  }

  private List<MarketDataProto.Instrument> generateTestInstruments2() {
    return Lists.newArrayList(
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.1)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("bns.to")
            .name("bank of nova scotia")
            .qty(20)
            .price(100)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(90)
            .date(20240920)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(5)
            .price(70)
            .direction(SELL)
            .date(20240913) // forced fudging
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(90)
            .date(20240920)
            .accountType(MarketDataProto.AccountType.NR)
            .build()
            .getInstrument());
  }

  private List<MarketDataProto.Instrument> generateTestInstruments3() {
    return Lists.newArrayList(
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.1)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.3)
            .date(20240910)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(90)
            .date(20240913)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(5)
            .price(70)
            .direction(SELL)
            .date(20240912) // forced fudging
            .build()
            .getInstrument());
  }

  private List<MarketDataProto.Instrument> generateTestInstruments4WithLastBlockAsFullSellOff() {
    return Lists.newArrayList(
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.1)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.3)
            .date(20240910)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(20)
            .price(70)
            .direction(SELL)
            .date(20240912)
            .build()
            .getInstrument());
  }

  private List<MarketDataProto.Instrument>
      generateTestInstruments5WithIntermediateSellAndLastBlockAsFullSellOff() {
    return Lists.newArrayList(
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.1)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(50.3)
            .date(20240910)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(5)
            .price(70)
            .direction(SELL)
            .date(20240912)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(65)
            .date(20240913)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(25)
            .price(120)
            .direction(SELL)
            .date(20240916)
            .build()
            .getInstrument());
  }

  private List<MarketDataProto.Instrument> generateTestDividendInstruments() {
    return Lists.newArrayList(
        TestInstrument.builder()
            .metadata(
                ImmutableMap.of(
                    "orderId", "DIVIDEND_20240912-TFSA-0-CM.TO", "isManufactured", "false"))
            .symbol("cm.to")
            .price(10)
            .date(20240912)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .metadata(
                ImmutableMap.of(
                    "orderId", "DIVIDEND_20240912-NR-0-CM.TO", "isManufactured", "false"))
            .symbol("cm.to")
            .price(20)
            .accountType(MarketDataProto.AccountType.NR)
            .date(20240912)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .metadata(
                ImmutableMap.of(
                    "orderId", "DIVIDEND_20240914-TFSA-0-CM.TO", "isManufactured", "false"))
            .symbol("cm.to")
            .price(5)
            .date(20240914)
            .build()
            .getInstrument() // out of mkt date
        );
  }
}
