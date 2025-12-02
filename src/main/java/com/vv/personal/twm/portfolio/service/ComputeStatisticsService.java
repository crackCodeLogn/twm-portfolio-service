package com.vv.personal.twm.portfolio.service;

import com.google.common.collect.Table;
import java.util.List;
import java.util.Optional;

/**
 * @author Vivek
 * @since 2025-11-30
 */
public interface ComputeStatisticsService {

  Optional<Double> computeCorrelation(String instrument1, String instrument2, List<Integer> dates);

  Optional<Table<String, String, Double>> computeCorrelationMatrix(
      List<String> instruments, List<Integer> dates);

  void cleanUp();
}
