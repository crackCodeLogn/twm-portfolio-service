package com.vv.personal.twm.portfolio.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Lists;
import com.vv.personal.twm.portfolio.model.market.InvestmentDivWeight;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Vivek
 * @since 2024-11-11
 */
@ExtendWith(MockitoExtension.class)
class InvestmentDivWeightServiceImplTest {

  @Mock private MarketDataPythonEngineFeign marketDataPythonEngineFeign;
  @Mock private MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;
  @Mock private TickerDataWarehouseService tickerDataWarehouseService;
  @InjectMocks private InvestmentDivWeightServiceImpl investmentDivWeightServiceImpl;

  @Test
  public void testCalcInvestmentBasedOnDivWeight_Empty() {
    Optional<InvestmentDivWeight> investmentDivWeight =
        investmentDivWeightServiceImpl.calcInvestmentBasedOnDivWeight(
            Lists.newArrayList(), Lists.newArrayList(), 100.0, 20.0, 25.0);
    assertTrue(investmentDivWeight.isEmpty());
  }

  @Test
  public void testComputeDivWeights() {}
}
