package com.vv.personal.twm.portfolio.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.cache.InstrumentMetaDataCache;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.util.TextReaderUtil;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Vivek
 * @since 2025-12-26
 */
@ExtendWith(MockitoExtension.class)
class InstrumentMetaDataServiceImplTest {

  @Mock private InstrumentMetaDataCache instrumentMetaDataCache;
  @Mock private MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;
  @Mock private MarketDataPythonEngineFeign marketDataPythonEngineFeign;

  private InstrumentMetaDataServiceImpl instrumentMetaDataService;

  @BeforeEach
  void setUp() {
    instrumentMetaDataService =
        new InstrumentMetaDataServiceImpl(
            instrumentMetaDataCache, marketDataCrdbServiceFeign, marketDataPythonEngineFeign);
  }

  @Test
  void queryInfo_cibc() {
    List<String> fileLines = TextReaderUtil.readTextLines("src/test/resources/cibcInfo.json");
    StringBuilder fileInfo = new StringBuilder();
    for (String line : fileLines) fileInfo.append(line).append("\n");
    String cibcInfo = fileInfo.toString();

    Mockito.when(marketDataPythonEngineFeign.getTickerInfo("CM.TO")).thenReturn(cibcInfo);
    MarketDataProto.Instrument.Builder imntBuilder = MarketDataProto.Instrument.newBuilder();

    instrumentMetaDataService.queryInfo("CM.TO", imntBuilder);
    System.out.println(imntBuilder);
    assertEquals(2, imntBuilder.getCorporateActionsCount());
    assertEquals(10, imntBuilder.getCompanyOfficersCount());
    assertEquals("1.289", imntBuilder.getMetaDataMap().get("beta"));
    assertEquals("EQUITY", imntBuilder.getMetaDataMap().get("quoteType"));
    assertEquals("67.375", imntBuilder.getMetaDataMap().get("bookValue")); // USD denominated
    assertEquals("2.4 - Buy", imntBuilder.getMetaDataMap().get("averageAnalystRating"));
  }

  @Test
  void queryInfo_vfv() {
    List<String> fileLines = TextReaderUtil.readTextLines("src/test/resources/vfvInfo.json");
    StringBuilder fileInfo = new StringBuilder();
    for (String line : fileLines) fileInfo.append(line).append("\n");
    String vfvInfo = fileInfo.toString();

    Mockito.when(marketDataPythonEngineFeign.getTickerInfo("VFV.TO")).thenReturn(vfvInfo);
    MarketDataProto.Instrument.Builder imntBuilder = MarketDataProto.Instrument.newBuilder();

    instrumentMetaDataService.queryInfo("VFV.TO", imntBuilder);
    System.out.println(imntBuilder);
    assertEquals(0, imntBuilder.getCorporateActionsCount());
    assertEquals(0, imntBuilder.getCompanyOfficersCount());
    assertEquals("0.95", imntBuilder.getMetaDataMap().get("beta3Year"));
    assertEquals("ETF", imntBuilder.getMetaDataMap().get("quoteType"));
    assertEquals("230240", imntBuilder.getMetaDataMap().get("volume"));
    assertNull(imntBuilder.getMetaDataMap().get("averageAnalystRating"));
  }

  @Test
  void queryInfo_cadusd() {
    List<String> fileLines = TextReaderUtil.readTextLines("src/test/resources/cadusdInfo.json");
    StringBuilder fileInfo = new StringBuilder();
    for (String line : fileLines) fileInfo.append(line).append("\n");
    String vfvInfo = fileInfo.toString();

    Mockito.when(marketDataPythonEngineFeign.getTickerInfo("CADUSD=X")).thenReturn(vfvInfo);
    MarketDataProto.Instrument.Builder imntBuilder = MarketDataProto.Instrument.newBuilder();

    instrumentMetaDataService.queryInfo("CADUSD=X", imntBuilder);
    System.out.println(imntBuilder);
    assertEquals(0, imntBuilder.getCorporateActionsCount());
    assertEquals(0, imntBuilder.getCompanyOfficersCount());
    assertEquals("CURRENCY", imntBuilder.getMetaDataMap().get("quoteType"));
    assertEquals("0", imntBuilder.getMetaDataMap().get("volume"));
  }
}
