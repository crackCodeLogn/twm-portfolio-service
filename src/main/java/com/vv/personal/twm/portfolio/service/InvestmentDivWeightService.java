package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.portfolio.model.market.InvestmentDivWeight;
import java.util.List;
import java.util.Optional;

/**
 * @author Vivek
 * @since 2024-11-11
 */
public interface InvestmentDivWeightService {

  Optional<InvestmentDivWeight> calcInvestmentBasedOnDivWeight(
      List<String> highValueTickerList,
      List<String> highDivTickerList,
      Double income,
      Double incomeSplitPercentage,
      Double amountToConsiderForDistributionTfsa);
}
