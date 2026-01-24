package com.vv.personal.twm.portfolio.util.math;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Think about shifting this class to twm-calc-service #todo */
public final class StatisticsUtil {

  private StatisticsUtil() {}

  public static Optional<Double> calculateMean(List<Double> inputValues) {
    if (inputValues == null || inputValues.isEmpty()) return Optional.empty();

    double mean = 0.0;
    for (int i = 0; i < inputValues.size(); i++) mean += inputValues.get(i);
    return Optional.of(mean / inputValues.size());
  }

  public static Optional<Double> calculateStandardDeviation(List<Double> inputValues) {
    return calculateStandardDeviation(inputValues, Optional.empty());
  }

  public static Optional<Double> calculateStandardDeviation(
      List<Double> inputValues, Optional<Double> inputMean) {
    Optional<Double> variance = calculateVariance(inputValues, inputMean);
    return variance.map(Math::sqrt);
  }

  public static Optional<Double> calculateStandardDeviation(Optional<Double> inputVariance) {
    return inputVariance.map(Math::sqrt);
  }

  public static Optional<Double> calculateVariance(
      List<Double> inputValues, Optional<Double> inputMean) {
    if (inputValues == null || inputValues.size() < 2) return Optional.empty();
    Optional<Double> mean = inputMean.isEmpty() ? calculateMean(inputValues) : inputMean;
    if (mean.isEmpty()) return mean;

    double standardDeviation = 0.0;
    for (int i = 0; i < inputValues.size(); i++)
      standardDeviation += Math.pow(inputValues.get(i) - mean.get(), 2);
    return Optional.of(standardDeviation / (inputValues.size() - 1));
  }

  public static Optional<Double> calculateCoVariance(
      List<Double> inputValues1, List<Double> inputValues2) {
    return calculateCoVariance(inputValues1, Optional.empty(), inputValues2, Optional.empty());
  }

  public static Optional<Double> calculateCoVariance(
      List<Double> inputValues1,
      Optional<Double> inputMean1,
      List<Double> inputValues2,
      Optional<Double> inputMean2) {
    if (inputValues1 == null
        || inputValues1.isEmpty()
        || inputValues2 == null
        || inputValues2.isEmpty()
        || inputValues1.size() != inputValues2.size()
        || inputValues1.size() < 2) return Optional.empty();

    if (inputMean1.isEmpty()) inputMean1 = calculateMean(inputValues1);
    if (inputMean2.isEmpty()) inputMean2 = calculateMean(inputValues2);
    if (inputMean1.isEmpty() || inputMean2.isEmpty()) return Optional.empty();

    double covariance = 0.0;
    for (int i = 0; i < inputValues1.size(); i++)
      covariance +=
          (inputValues1.get(i) - inputMean1.get()) * (inputValues2.get(i) - inputMean2.get());
    return Optional.of(covariance / (inputValues1.size() - 1));
  }

  /** Ingests list of N values and returns a list of N-1 logarithmic returns */
  public static Optional<List<Double>> calculateLogarithmicDelta(List<Double> inputValues) {
    if (inputValues == null || inputValues.size() <= 1) return Optional.empty();

    List<Double> logarithmicDelta = new ArrayList<>(inputValues.size() - 1);
    for (int i = 1; i < inputValues.size(); i++)
      // logarithmicDelta.add(Math.log(inputValues.get(i)) - Math.log(inputValues.get(i - 1)));
      logarithmicDelta.add(
          Math.log(inputValues.get(i) / inputValues.get(i - 1))); // same as above, optimized
    // assuming stock price never reaches 0.0 cause then we have bigger problems to worry about
    return Optional.of(logarithmicDelta);
  }

  public static void calculateParallelLogarithmicDelta(List<Double> inputValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static Optional<Double> calculateCorrelation(
      List<Double> inputValues1, List<Double> inputValues2) {
    Optional<Double> covariance = calculateCoVariance(inputValues1, inputValues2);
    if (covariance.isEmpty()) return covariance;

    Optional<Double> standardDeviation1 = calculateStandardDeviation(inputValues1);
    Optional<Double> standardDeviation2 = calculateStandardDeviation(inputValues2);
    if (standardDeviation1.isEmpty() || standardDeviation2.isEmpty()) return Optional.empty();

    return Optional.of(covariance.get() / (standardDeviation1.get() * standardDeviation2.get()));
  }

  public static Optional<Double> calculateChangePercentage(
      Optional<Double> startValue, Optional<Double> endValue) {
    if (startValue.isEmpty() || endValue.isEmpty()) return Optional.empty();
    return Optional.of((endValue.get() - startValue.get()) / startValue.get() * 100.0);
  }
}
