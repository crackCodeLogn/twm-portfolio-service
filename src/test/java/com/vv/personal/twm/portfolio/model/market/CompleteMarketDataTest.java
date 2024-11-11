package com.vv.personal.twm.portfolio.model.market;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.Direction.SELL;
import static com.vv.personal.twm.portfolio.TestConstants.DELTA_PRECISION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
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
    assertEquals(51, cibcTfsaList.getHead().getAcb().getTotalAcb());
    // special because apparently the acb went negative due to test data
    assertEquals(0, cibcTfsaList.getHead().getNext().getAcb().getTotalAcb());
    assertEquals(150, cibcTfsaList.getTail().getAcb().getTotalAcb());

    DataList cmNrList = result.get("CM.TO").get(MarketDataProto.AccountType.NR);
    assertNotNull(cmNrList);
    assertEquals(1, cmNrList.getBlocks());
    assertEquals(15, cmNrList.getHead().getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(150, cmNrList.getHead().getAcb().getTotalAcb(), DELTA_PRECISION);

    assertTrue(result.containsKey("BNS.TO"));
    DataList bnsTfsaList = result.get("BNS.TO").get(MarketDataProto.AccountType.TFSA);
    assertNotNull(bnsTfsaList);
    assertEquals(1, bnsTfsaList.getBlocks());
    assertEquals(10, bnsTfsaList.getHead().getAcb().getAcbPerShare(), DELTA_PRECISION);
    assertEquals(200, bnsTfsaList.getHead().getAcb().getTotalAcb(), DELTA_PRECISION);
  }

  @Test
  public void testComputePnL() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder()
            .addAllInstruments(generateTestInstruments2())
            .build();
    System.out.println(portfolio);

    marketData.populate(portfolio);
    marketData.computeAcb();
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240909)).thenReturn(5.2);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240913)).thenReturn(7.0);
    when(tickerDataWarehouseService.getMarketData("CM.TO", 20240920)).thenReturn(9.0);
    when(tickerDataWarehouseService.getMarketData("BNS.TO", 20240909)).thenReturn(10.1);
    marketData.setTickerDataWarehouseService(tickerDataWarehouseService);

    marketData.computePnL();
    Map<Integer, Map<MarketDataProto.AccountType, Double>> realizedPnLMap =
        marketData.getRealizedPnLMap();
    Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedPnLMap =
        marketData.getUnrealizedPnLMap();
    assertFalse(realizedPnLMap.isEmpty());
    assertFalse(unrealizedPnLMap.isEmpty());
    System.out.println(realizedPnLMap);
    System.out.println(unrealizedPnLMap);
  }

  private List<MarketDataProto.Instrument> generateTestInstruments() {
    return Lists.newArrayList(
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(5.1)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("bns.to")
            .name("bank of nova scotia")
            .qty(20)
            .price(10)
            .date(20240912)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(15)
            .date(20240920)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(5)
            .price(12)
            .direction(SELL)
            .date(20240913) // forced fudging
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(15)
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
            .price(5.1)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("bns.to")
            .name("bank of nova scotia")
            .qty(20)
            .price(10)
            .date(20240909)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(9)
            .date(20240920)
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(5)
            .price(7)
            .direction(SELL)
            .date(20240913) // forced fudging
            .build()
            .getInstrument(),
        TestInstrument.builder()
            .symbol("cm.to")
            .name("cibc")
            .qty(10)
            .price(9)
            .date(20240920)
            .accountType(MarketDataProto.AccountType.NR)
            .build()
            .getInstrument());
  }
}
