package com.vv.personal.twm.portfolio.config;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.AccountType.NR;
import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.AccountType.TFSA;
import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.InstrumentType.EQUITY;
import static com.vv.personal.twm.portfolio.TestConstants.PRECISION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.AdjustedCostBase2;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataEngineFeign;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Vivek
 * @since 2024-08-06
 */
@ExtendWith(MockitoExtension.class)
class DataConfigTest {

  @Mock private MarketDataEngineFeign marketDataEngineFeign;

  @InjectMocks private DataConfig dataConfig;

  @Test
  public void testBeanSupplier() {
    MarketDataProto.Portfolio portfolio =
        MarketDataProto.Portfolio.newBuilder()
            .addInstruments(
                generateInstrument("SU.TO", "Suncor", "energy", EQUITY, 20240806, 10.34, 20, TFSA))
            .addInstruments(
                generateInstrument("SU.TO", "Suncor", "energy", EQUITY, 20240806, 1.34, 20, NR))
            .addInstruments(
                generateInstrument("SU.TO", "Suncor", "energy", EQUITY, 20240807, 20, 20, TFSA))
            .build();

    when(marketDataEngineFeign.getPortfolioData(anyString())).thenReturn(portfolio);
    AdjustedCostBase2 adjustedCostBase2 = dataConfig.createAdjustedCostBase();
    assertNotNull(adjustedCostBase2);

    assertEquals(15.17, adjustedCostBase2.getAdjustedCost("SU.TO", TFSA), PRECISION);
    assertEquals(1.34, adjustedCostBase2.getAdjustedCost("SU.TO", NR), PRECISION);
  }

  private MarketDataProto.Instrument generateInstrument(
      String symbol,
      String name,
      String sector,
      MarketDataProto.InstrumentType instrumentType,
      int date,
      double price,
      double qty,
      MarketDataProto.AccountType accountType) {
    return MarketDataProto.Instrument.newBuilder()
        .setTicker(
            MarketDataProto.Ticker.newBuilder()
                .setSymbol(symbol)
                .setName(name)
                .setSector(sector)
                .setType(instrumentType)
                .addData(MarketDataProto.Value.newBuilder().setDate(date).setPrice(price).build())
                .build())
        .setQty(qty)
        .setAccountType(accountType)
        .build();
  }
}
