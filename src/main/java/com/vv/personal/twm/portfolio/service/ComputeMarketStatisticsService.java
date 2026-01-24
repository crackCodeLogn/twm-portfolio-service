package com.vv.personal.twm.portfolio.service;

import com.google.common.collect.Table;
import java.util.List;
import java.util.Optional;

/**
 * @author Vivek
 * @since 2025-11-30
 */
public interface ComputeMarketStatisticsService {

  Double computeExpectedReturn(Double beta, Double riskFreeReturn, Double marketReturn);

  Optional<Double> computeBeta(String instrument, String marketSymbol, List<Integer> dates);

  Optional<Double> computeStandardDeviationInFormOfEWMAVol(String instrument, List<Integer> dates);

  Optional<Double> computeLatestMovingAverage(
      String instrument, int timeFrame, List<Integer> dates);

  Optional<Double> computeCorrelation(String instrument1, String instrument2, List<Integer> dates);

  Optional<Table<String, String, Double>> computeCorrelationMatrix(
      List<String> instruments, List<Integer> dates);

  void cleanUp();
}
