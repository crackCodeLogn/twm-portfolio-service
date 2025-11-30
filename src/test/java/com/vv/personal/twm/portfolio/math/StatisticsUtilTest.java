package com.vv.personal.twm.portfolio.math;

import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.portfolio.util.math.StatisticsUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatisticsUtilTest {

  // Tolerance for double comparisons, standard for statistical tests
  private final double DELTA = 0.0001;

  // --- Data Setup ---

  // A standard list of numbers: [2, 4, 4, 4, 5, 5, 7, 9] (N=8)
  // Mean (µ): 5.0, Population Variance (σ²): 4.0, Population Standard Deviation (σ): 2.0
  private final List<Double> SIMPLE_DATA = Arrays.asList(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0);
  private final List<Double> EMPTY_DATA = Collections.emptyList();
  private final List<Double> SINGLE_VALUE_DATA = Collections.singletonList(10.0);

  // Logarithmic data: [10, 20, 40, 80]
  // Log Deltas: [ln(2), ln(2), ln(2)]
  private final List<Double> LOG_DATA = Arrays.asList(10.0, 20.0, 40.0, 80.0);

  // Covariance Data (N=5)
  // X = [1, 2, 3, 4, 5], Mean_X = 3.0
  private final List<Double> COV_X_DATA = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
  // Y = [5, 4, 3, 2, 1], Mean_Y = 3.0, Population Covariance(X, Y) = -2.0
  private final List<Double> COV_Y_NEG_DATA = Arrays.asList(5.0, 4.0, 3.0, 2.0, 1.0);

  // Invalid Inputs
  private final List<Double> MISMATCHED_SIZE_DATA = Arrays.asList(1.0, 2.0); // Size 2
  private final Optional<Double> EMPTY_MEAN = Optional.empty();
  private final Optional<Double> MEAN_3_0 = Optional.of(3.0);
  private final Optional<Double> MEAN_5_0 = Optional.of(5.0);

  // -------------------------------------------------------------------------------------------------------------
  // ## calculateMean Tests
  // -------------------------------------------------------------------------------------------------------------

  @Test
  void calculateMean_ShouldReturnCorrectMean() {
    // Test with SIMPLE_DATA, expected mean 5.0
    Optional<Double> result = StatisticsUtil.calculateMean(SIMPLE_DATA);
    assertTrue(result.isPresent());
    assertEquals(5.0, result.get(), DELTA);
  }

  @Test
  void calculateMean_ShouldHandleSingleValue() {
    // Mean of [10.0] is 10.0
    Optional<Double> result = StatisticsUtil.calculateMean(SINGLE_VALUE_DATA);
    assertTrue(result.isPresent());
    assertEquals(10.0, result.get(), DELTA);
  }

  @Test
  void calculateMean_ShouldReturnEmptyForNullInput() {
    Optional<Double> result = StatisticsUtil.calculateMean(null);
    assertTrue(result.isEmpty());
  }

  @Test
  void calculateMean_ShouldReturnEmptyForEmptyInput() {
    Optional<Double> result = StatisticsUtil.calculateMean(EMPTY_DATA);
    assertTrue(result.isEmpty());
  }

  // -------------------------------------------------------------------------------------------------------------
  // ## calculateVariance Tests
  // -------------------------------------------------------------------------------------------------------------

  @Test
  void calculateVariance_ShouldReturnCorrectPopulationVariance_WithoutMean() {
    // Test with SIMPLE_DATA, expected variance 4.0
    Optional<Double> result = StatisticsUtil.calculateVariance(SIMPLE_DATA, EMPTY_MEAN);
    assertTrue(result.isPresent());
    assertEquals(4.0, result.get(), DELTA);
  }

  @Test
  void calculateVariance_ShouldReturnCorrectPopulationVariance_WithPrecalculatedMean() {
    // Test with SIMPLE_DATA and pre-calculated mean 5.0, expected variance 4.0
    Optional<Double> result = StatisticsUtil.calculateVariance(SIMPLE_DATA, MEAN_5_0);
    assertTrue(result.isPresent());
    assertEquals(4.0, result.get(), DELTA);
  }

  @Test
  void calculateVariance_ShouldReturnZeroForSingleValue() {
    // Variance for a single value [10.0] is 0.0
    Optional<Double> result = StatisticsUtil.calculateVariance(SINGLE_VALUE_DATA, EMPTY_MEAN);
    assertTrue(result.isPresent());
    assertEquals(0.0, result.get(), DELTA);
  }

  @Test
  void calculateVariance_ShouldReturnEmptyForNullInput() {
    Optional<Double> result = StatisticsUtil.calculateVariance(null, EMPTY_MEAN);
    assertTrue(result.isEmpty());
  }

  @Test
  void calculateVariance_ShouldReturnEmptyForEmptyInput() {
    Optional<Double> result = StatisticsUtil.calculateVariance(EMPTY_DATA, EMPTY_MEAN);
    assertTrue(result.isEmpty());
  }

  // -------------------------------------------------------------------------------------------------------------
  // ## calculateStandardDeviation Tests
  // -------------------------------------------------------------------------------------------------------------

  @Test
  void calculateStandardDeviation_NoMean_ShouldReturnCorrectPopulationStandardDeviation() {
    // Test with SIMPLE_DATA, expected result sqrt(4.0) = 2.0
    Optional<Double> result = StatisticsUtil.calculateStandardDeviation(SIMPLE_DATA);
    assertTrue(result.isPresent());
    assertEquals(2.0, result.get(), DELTA);
  }

  @Test
  void calculateStandardDeviation_WithMean_ShouldReturnCorrectPopulationStandardDeviation() {
    // Test with SIMPLE_DATA and pre-calculated mean 5.0, expected result 2.0
    Optional<Double> result = StatisticsUtil.calculateStandardDeviation(SIMPLE_DATA, MEAN_5_0);
    assertTrue(result.isPresent());
    assertEquals(2.0, result.get(), DELTA);
  }

  @Test
  void calculateStandardDeviation_ShouldReturnZeroForSingleValue() {
    // Standard Deviation for a single value [10.0] is 0.0
    Optional<Double> result = StatisticsUtil.calculateStandardDeviation(SINGLE_VALUE_DATA);
    assertTrue(result.isPresent());
    assertEquals(0.0, result.get(), DELTA);
  }

  @Test
  void calculateStandardDeviation_ShouldReturnEmptyForNullInput() {
    Optional<Double> result = StatisticsUtil.calculateStandardDeviation(null);
    assertTrue(result.isEmpty());
  }

  // -------------------------------------------------------------------------------------------------------------
  // ## calculateCoVariance Tests
  // -------------------------------------------------------------------------------------------------------------

  @Test
  void calculateCoVariance_ShouldCalculateCorrectNegativeCovariance_WithoutMeans() {
    // Test X and Y (negative correlation) -> Expected -2.0
    Optional<Double> result =
        StatisticsUtil.calculateCoVariance(COV_X_DATA, EMPTY_MEAN, COV_Y_NEG_DATA, EMPTY_MEAN);

    assertTrue(result.isPresent());
    assertEquals(-2.0, result.get(), DELTA);
  }

  @Test
  void calculateCoVariance_ShouldCalculateCorrectCovariance_WithPrecalculatedMeans() {
    // Test X and X (positive correlation) with means 3.0 and 3.0 -> Expected 2.0
    Optional<Double> result =
        StatisticsUtil.calculateCoVariance(COV_X_DATA, MEAN_3_0, COV_X_DATA, MEAN_3_0);

    assertTrue(result.isPresent());
    assertEquals(2.0, result.get(), DELTA);
  }

  @Test
  void calculateCoVariance_ShouldCalculateCorrectCovariance_WithMixedMeansInput() {
    // Test X and Y: X mean supplied, Y mean calculated internally. Expected -2.0
    Optional<Double> result =
        StatisticsUtil.calculateCoVariance(COV_X_DATA, MEAN_3_0, COV_Y_NEG_DATA, EMPTY_MEAN);

    assertTrue(result.isPresent());
    assertEquals(-2.0, result.get(), DELTA);
  }

  @Test
  void calculateCoVariance_ShouldReturnEmptyForMismatchedSizes() {
    Optional<Double> result =
        StatisticsUtil.calculateCoVariance(
            COV_X_DATA, EMPTY_MEAN, MISMATCHED_SIZE_DATA, EMPTY_MEAN);
    assertTrue(result.isEmpty());
  }

  @Test
  void calculateCoVariance_ShouldReturnEmptyForNullInput1() {
    Optional<Double> result =
        StatisticsUtil.calculateCoVariance(null, EMPTY_MEAN, COV_X_DATA, EMPTY_MEAN);
    assertTrue(result.isEmpty());
  }

  @Test
  void calculateCoVariance_ShouldReturnEmptyForEmptyInput2() {
    Optional<Double> result =
        StatisticsUtil.calculateCoVariance(COV_X_DATA, EMPTY_MEAN, EMPTY_DATA, EMPTY_MEAN);
    assertTrue(result.isEmpty());
  }

  // -------------------------------------------------------------------------------------------------------------
  // ## calculateLogarithmicDelta Tests
  // -------------------------------------------------------------------------------------------------------------

  @Test
  void calculateLogarithmicDelta_ShouldReturnCorrectDeltas() {
    // Input: [10.0, 20.0, 40.0, 80.0]. Expected: [ln(2), ln(2), ln(2)]
    Optional<List<Double>> result = StatisticsUtil.calculateLogarithmicDelta(LOG_DATA);
    assertTrue(result.isPresent());
    List<Double> deltas = result.get();

    assertEquals(3, deltas.size());
    double expectedDelta = Math.log(2.0);
    assertEquals(expectedDelta, deltas.get(0), DELTA);
    assertEquals(expectedDelta, deltas.get(1), DELTA);
    assertEquals(expectedDelta, deltas.get(2), DELTA);
  }

  @Test
  void calculateLogarithmicDelta_ShouldReturnEmptyForNullInput() {
    Optional<List<Double>> result = StatisticsUtil.calculateLogarithmicDelta(null);
    assertTrue(result.isEmpty());
  }

  @Test
  void calculateLogarithmicDelta_ShouldReturnEmptyForSingleValueInput() {
    Optional<List<Double>> result = StatisticsUtil.calculateLogarithmicDelta(SINGLE_VALUE_DATA);
    assertTrue(result.isEmpty());
  }

  // -------------------------------------------------------------------------------------------------------------
  // ## calculateParallelLogarithmicDelta Tests
  // -------------------------------------------------------------------------------------------------------------

  @Test
  void calculateParallelLogarithmicDelta_ShouldThrowUnsupportedOperationException() {
    // Test for the method that is explicitly not supported
    assertThrows(
        UnsupportedOperationException.class,
        () -> StatisticsUtil.calculateParallelLogarithmicDelta(SINGLE_VALUE_DATA));
  }
}
