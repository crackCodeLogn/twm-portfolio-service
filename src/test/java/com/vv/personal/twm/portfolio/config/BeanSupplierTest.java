package com.vv.personal.twm.portfolio.config;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.AdjustedCostBase;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataEngineFeign;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.AccountType.NR;
import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.AccountType.TFSA;
import static com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto.InstrumentType.EQUITY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * @author Vivek
 * @since 2024-08-06
 */
@ExtendWith(MockitoExtension.class)
class BeanSupplierTest {
    private static final double PRECISION = Math.pow(10, -6);

    @Mock
    private MarketDataEngineFeign marketDataEngineFeign;

    @InjectMocks
    private BeanSupplier beanSupplier;

    @Test
    public void testBeanSupplier() {
        MarketDataProto.Portfolio portfolio = MarketDataProto.Portfolio.newBuilder()
                .addInstruments(generateInstrument("SU.TO", "Suncor", "energy", EQUITY, "2024-08-06", 10.34, 20, TFSA))
                .addInstruments(generateInstrument("SU.TO", "Suncor", "energy", EQUITY, "2024-08-06", 1.34, 20, NR))
                .addInstruments(generateInstrument("SU.TO", "Suncor", "energy", EQUITY, "2024-08-07", 20, 20, TFSA))
                .build();

        when(marketDataEngineFeign.getPortfolioData(anyString())).thenReturn(portfolio);
        AdjustedCostBase adjustedCostBase = beanSupplier.createAdjustedCostBase();
        assertNotNull(adjustedCostBase);

        assertEquals(15.17, adjustedCostBase.getAdjustedCost("SU.TO", TFSA), PRECISION);
        assertEquals(1.34, adjustedCostBase.getAdjustedCost("SU.TO", NR), PRECISION);
    }

    private MarketDataProto.Instrument generateInstrument(String symbol, String name, String sector, MarketDataProto.InstrumentType instrumentType, String date, double price, double qty, MarketDataProto.AccountType accountType) {
        return MarketDataProto.Instrument.newBuilder()
                .setTicker(MarketDataProto.Ticker.newBuilder()
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