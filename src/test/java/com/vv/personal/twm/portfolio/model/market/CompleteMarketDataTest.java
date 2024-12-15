package com.vv.personal.twm.portfolio.model.market;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.SELL;
import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.util.TestInstrument;
import java.util.List;
import java.util.Map;
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
class CompleteMarketDataTest {

  @Mock private TickerDataWarehouseService tickerDataWarehouseService;

  private CompleteMarketData marketData;

  @BeforeEach
  void setUp() {
    marketData = new CompleteMarketData();
  }

  @Test
  public void populateMap() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder().addAllInstruments(generateTestInstruments()).build();
    System.out.println(portfolio);

    marketData.populate(portfolio);
    marketData.computeAcb();
    Map<String, Map<MarketDataProto.AccountType, DataList>> result = marketData.getMarketData();

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
  public void testComputePnL() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder()
            .addAllInstruments(generateTestInstruments3())
            .build();
    System.out.println(portfolio);
    marketData.populate(portfolio);
    marketData.computeAcb(); // at this point, assumption is that acb compute is accurate

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

    marketData.setTickerDataWarehouseService(tickerDataWarehouseService);
    marketData.computePnL();
    Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedPnLMap =
        marketData.getUnrealizedDatePnLMap();
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
        marketData.getUnrealizedImntPnLMap();
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
        marketData.getRealizedDatePnLMap();
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
        marketData.getRealizedImntPnLMap();
    System.out.println("realizedImntPnLMap = " + realizedImntPnLMap);
    assertFalse(realizedImntPnLMap.isEmpty());
    assertTrue(realizedImntPnLMap.containsKey("CM.TO"));
    Map<Integer, Double> realizedImntPnLCibcMap =
        realizedImntPnLMap.get("CM.TO").get(MarketDataProto.AccountType.TFSA);
    assertEquals(9.9, realizedImntPnLCibcMap.get(20240912), DELTA_PRECISION);

    Map<Integer, Map<MarketDataProto.AccountType, Double>> combinedPnLMap =
        marketData.getCombinedDatePnLMap();
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
}
