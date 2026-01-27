package com.vv.personal.twm.portfolio.service.impl;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.vv.personal.twm.portfolio.cache.DateLocalDateCache;
import com.vv.personal.twm.portfolio.service.ComputeMarketStatisticsService;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.math.StatisticsUtil;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2025-11-30
 */
@Slf4j
@AllArgsConstructor
@Service
public class ComputeMarketStatisticsServiceImpl implements ComputeMarketStatisticsService {
  private static final double LAMBDA_EWMA_VOL = .94;
  private final DateLocalDateCache dateLocalDateCache;
  private final TickerDataWarehouseService tickerDataWarehouseService;

  private final Map<ImntDatesRecord, Optional<List<Double>>> tmpImntDatesRecordListMap =
      new ConcurrentHashMap<>();

  /** CAPM */
  @Override
  public Double computeExpectedReturn(Double beta, Double riskFreeReturn, Double marketReturn) {
    return riskFreeReturn + beta * (marketReturn - riskFreeReturn);
  }

  @Override
  public Optional<Double> computeBeta(String instrument, String marketSymbol, List<Integer> dates) {
    List<Double> imntValues = new ArrayList<>(dates.size());
    List<Double> marketValues = new ArrayList<>(dates.size());

    for (Integer date : dates) {
      LocalDate localDate = dateLocalDateCache.getOrCalc(date);
      Double imntPrice = tickerDataWarehouseService.getMarketData(instrument, localDate);
      Double marketPrice = tickerDataWarehouseService.getMarketData(marketSymbol, localDate);
      if (imntPrice != null && marketPrice != null) {
        imntValues.add(imntPrice);
        marketValues.add(marketPrice);
      }
    }

    Optional<Double> covariance = StatisticsUtil.calculateCoVariance(imntValues, marketValues);
    if (covariance.isPresent()) {
      Optional<Double> variance = StatisticsUtil.calculateVariance(marketValues, Optional.empty());
      if (variance.isPresent()) {
        return Optional.of(covariance.get() / variance.get());
      }
    }
    return Optional.empty();
  }

  /**
   * Computes the standard deviation of an imnt using the EWMA (Exponentially Weighted Moving
   * Average) volatility. <br>
   * Concept is explained in detail here: https://www.investopedia.com/articles/07/ewma.asp,
   * https://corporatefinanceinstitute.com/resources/career-map/sell-side/capital-markets/exponentially-weighted-moving-average-ewma/
   */
  @Override
  public Optional<Double> computeStandardDeviationInFormOfEWMAVol(
      String instrument, List<Integer> dates) {
    List<Double> imntValues = new ArrayList<>(dates.size());
    for (int i = 0; i < dates.size(); i++) {
      Double value = tickerDataWarehouseService.getMarketData(instrument, dates.get(i));
      if (value != null) imntValues.add(value);
    }
    Optional<List<Double>> logReturns = StatisticsUtil.calculateLogarithmicDelta(imntValues);
    if (logReturns.isPresent()) {
      double variance = Math.pow(logReturns.get().get(0), 2);
      final double COUNTER_LAMBDA = 1 - LAMBDA_EWMA_VOL;

      for (int i = 1; i < logReturns.get().size(); i++) {
        variance =
            LAMBDA_EWMA_VOL * variance + COUNTER_LAMBDA * Math.pow(logReturns.get().get(i), 2);
      }

      double dailyStdDev = Math.sqrt(variance);
      return Optional.of(dailyStdDev * Math.sqrt(252)); // annualization
      // cm -> 0.14254528859429927
    }
    return Optional.empty();
    /*if (logReturns.isPresent()) return StatisticsUtil.calculateStandardDeviation(logReturns.get());
    return Optional.empty();*/
    // cm -> 0.014620557819936247
    // return StatisticsUtil.calculateStandardDeviation(imntValues); // CM -> 21.523893573664377
  }

  /** Relative Strength Index */
  @Override
  public Optional<Double> computeRsi(String instrument, int timeFrame, List<Integer> dates) {
    if ("STLC.TO".equals(instrument)) return Optional.empty();

    double rsi = 50.0; // Default to neutral
    if (dates.size() <= timeFrame) return Optional.of(rsi);

    List<Double> prices = new ArrayList<>();
    int lookback = Math.min(dates.size(), 252);
    for (int i = dates.size() - lookback; i < dates.size(); i++)
      prices.add(tickerDataWarehouseService.getMarketData(instrument, dates.get(i)));

    if (prices.size() <= timeFrame) {
      log.warn("Lesser price points found for {} than the timeframe {}", instrument, timeFrame);
      return Optional.of(rsi);
    }

    double avgGain = 0;
    double avgLoss = 0;

    // Initial average
    for (int i = 1; i <= timeFrame; i++) {
      double change = prices.get(i) - prices.get(i - 1);
      if (change > 0) avgGain += change;
      else avgLoss += Math.abs(change);
    }
    avgGain /= timeFrame;
    avgLoss /= timeFrame;

    // Smoothed averages (Wilder's method)
    for (int i = timeFrame + 1; i < prices.size(); i++) {
      double change = prices.get(i) - prices.get(i - 1);
      double currentGain = Math.max(0, change);
      double currentLoss = Math.max(0, -change);

      avgGain = (avgGain * (timeFrame - 1) + currentGain) / timeFrame;
      avgLoss = (avgLoss * (timeFrame - 1) + currentLoss) / timeFrame;
    }

    if (avgLoss == 0) rsi = 100.0;
    else if (avgGain == 0) rsi = 0.0;
    else {
      double rs = avgGain / avgLoss;
      rsi = 100.0 - (100.0 / (1 + rs));
    }
    return Optional.of(rsi);
  }

  @Override
  public Double computeMaxWeight(
      String imnt,
      Optional<Double> currentPrice,
      List<Integer> integerDates,
      Optional<Double> yield) {
    double weight = .25;
    Optional<Double> ma200 = computeLatestMovingAverage(imnt, 200, integerDates);
    Optional<Double> ma50 = computeLatestMovingAverage(imnt, 50, integerDates);
    Optional<Double> rsi = computeRsi(imnt, 14, integerDates);

    if (ma200.isEmpty() || ma50.isEmpty() || rsi.isEmpty() || currentPrice.isEmpty()) {
      log.warn(
          "Cannot calculate max weight accurately due to one of the missing factors: ma200 {}, ma50 {}, rsi {}, t-price {}",
          ma200,
          ma50,
          rsi,
          currentPrice);
      return 0.0;
    }

    if (yield.isPresent() && yield.get() >= 7.0) {
      log.warn("Minimizing weight of {} because its yield is {}", imnt, yield.get());
      weight = .01;
    }
    // aggressive dip buy as it is undervalued
    else if (rsi.get() < 30) {
      weight = .35;
      if (currentPrice.get() < ma200.get() * .90) weight = .45;
    }
    // aggressive profit take by selling current positions and buy more from under undervalued imnts
    else if (currentPrice.get() > ma50.get() * 1.15 && rsi.get() > 70) weight = .10;
    return weight;
  }

  @Override
  public Optional<Double> computeLatestMovingAverage(
      String instrument, int timeFrame, List<Integer> dates) {
    if (dates == null || dates.isEmpty() || dates.size() < timeFrame || timeFrame <= 0) {
      log.warn(
          "Cannot compute moving average for {}x{}x{} dates",
          instrument,
          timeFrame,
          dates == null ? 0 : dates.size());
      return Optional.empty();
    }

    double sum = 0.0;
    int count = 0;
    for (int i = dates.size() - 1; i >= 0 && timeFrame-- > 0; i--) {
      LocalDate localDate = dateLocalDateCache.getOrCalc(dates.get(i));
      Double price = tickerDataWarehouseService.getMarketData(instrument, localDate);
      if (price != null) {
        sum += price;
        count++;
      }
    }
    return Optional.of(sum / count);
  }

  /**
   * Assumptions:
   *
   * <ol>
   *   <li>@param{dates} will be sorted
   *   <li>TickerDataWarehouse will have the pricing data for the queried date for the instruments
   *       in question // todo - handle this situation when imnts whose data is missing is queried
   * </ol>
   */
  @Override
  public Optional<Double> computeCorrelation(
      String instrument1, String instrument2, List<Integer> dates) {
    if (StringUtils.isEmpty(instrument1) || StringUtils.isEmpty(instrument2) || dates.isEmpty()) {
      log.error("Can't compute correlation for empty instruments or dates.");
      return Optional.empty();
    }
    if (instrument1.equals(instrument2)) return Optional.of(1.0);

    Optional<List<Double>> imnt1Values = Optional.empty();
    Optional<List<Double>> imnt2Values = Optional.empty();

    log.debug(
        "Firing up compute of correlation for imnts {} and {} for {} dates",
        instrument1,
        instrument2,
        dates.size());
    StopWatch timer = StopWatch.createStarted();

    Optional<Double> correlation = Optional.empty();
    try {
      ExecutorService dualExecutor = Executors.newFixedThreadPool(2);
      try {
        List<Future<Optional<List<Double>>>> imntValues =
            dualExecutor.invokeAll(
                Lists.newArrayList(
                    generateInstrumentValueReadFromWarehouseTask(instrument1, dates),
                    generateInstrumentValueReadFromWarehouseTask(instrument2, dates)),
                30,
                TimeUnit.SECONDS);
        dualExecutor.shutdown();

        if (imntValues.size() != 2) {
          log.error(
              "Failed to compute correlation properly between instruments {} and {} for {} dates",
              instrument1,
              instrument2,
              dates.size());
          return Optional.empty();
        }
        imnt1Values = imntValues.get(0).get();
        imnt2Values = imntValues.get(1).get();
      } catch (InterruptedException | ExecutionException e) {
        log.error(
            "Failed to successfully read imnt values for {} and {} for {} dates from warehouse",
            instrument1,
            instrument2,
            dates.size(),
            e);
      }
      if (imnt1Values.isEmpty() || imnt2Values.isEmpty()) {
        log.error(
            "Failed to compute correlation between instruments {} and {} for {} dates",
            instrument1,
            instrument2,
            dates.size());
        return Optional.empty();
      }

      correlation = StatisticsUtil.calculateCorrelation(imnt1Values.get(), imnt2Values.get());
      log.debug("Correlation between {} x {}: {}", instrument1, instrument2, correlation);
    } catch (Exception e) {
      log.error(
          "Failed to compute correlation between instruments {} x {} correctly",
          instrument1,
          instrument2,
          e);
    } finally {
      timer.stop();
      log.debug(
          "Correlation compute for instruments {} and {} for {} dates took {} ms",
          instrument1,
          instrument2,
          dates.size(),
          timer.getTime(TimeUnit.MILLISECONDS));
      timer = null;
    }
    return correlation;
  }

  @Override
  public Optional<Table<String, String, Double>> computeCorrelationMatrix(
      List<String> instruments, List<Integer> dates) {
    if (instruments.isEmpty() || dates.isEmpty()) {
      log.error("Can't compute correlation for empty instruments or dates.");
      return Optional.empty();
    }
    log.info("Activating correlation matrix computation for {} instruments", instruments.size());
    StopWatch timer = StopWatch.createStarted();

    Table<String, String, Double> correlationMatrix = HashBasedTable.create();
    // initial population fill up
    for (int i = 0; i < instruments.size(); i++)
      for (int j = 0; j < instruments.size(); j++)
        correlationMatrix.put(instruments.get(i), instruments.get(j), 0.0);

    try {
      ExecutorService heavyExecutor = Executors.newFixedThreadPool(8);

      List<Callable<Void>> correlationComputeTasks =
          new ArrayList<>(instruments.size() * instruments.size() / 2);
      for (int i = 0; i < instruments.size(); i++)
        for (int j = i; j < instruments.size(); j++)
          correlationComputeTasks.add(
              generateCorrelationComputeTask(
                  instruments.get(i), instruments.get(j), dates, correlationMatrix));
      log.info(
          "Generation of {} tasks ({}C2) for correlation compute completed",
          correlationComputeTasks.size(),
          instruments.size());

      log.info("Initiating heavy executor correlation compute now");
      try {
        heavyExecutor.invokeAll(correlationComputeTasks);
      } catch (InterruptedException e) {
        log.error(
            "Interrupted while running through the correlation compute tasks in the heavy executor",
            e);
      } finally {
        heavyExecutor.shutdown();
      }
    } finally {
      timer.stop();
      log.info("Correlation Matrix computation took {} ms", timer.getTime(TimeUnit.MILLISECONDS));
      timer = null;
    }

    return Optional.of(correlationMatrix);
  }

  @Override
  public void cleanUp() {
    tmpImntDatesRecordListMap.clear();
  }

  private Callable<Optional<List<Double>>> generateInstrumentValueReadFromWarehouseTask(
      String imnt, List<Integer> dates) {
    return () -> {
      ImntDatesRecord record =
          new ImntDatesRecord(imnt, dates.get(0), dates.get(dates.size() - 1), dates.size());
      if (tmpImntDatesRecordListMap.containsKey(record)) { // cache hit
        return tmpImntDatesRecordListMap.get(record);
      }

      List<Double> imntValues = new ArrayList<>(dates.size());
      for (int i = 0; i < dates.size(); i++) {
        Double value = tickerDataWarehouseService.getMarketData(imnt, dates.get(i));
        if (value != null) imntValues.add(value);
      }
      // convert the raw prices to log returns
      Optional<List<Double>> logImntValues = StatisticsUtil.calculateLogarithmicDelta(imntValues);
      imntValues = null; // dump memory

      if (logImntValues.isPresent()) {
        tmpImntDatesRecordListMap.put(record, logImntValues); // populate tmp cache
      } else {
        log.warn("Failed to calculate logarithmic delta for imnt {}", imnt);
      }
      log.debug("imnt {} => log returns {}", imnt, logImntValues);
      return logImntValues;
    };
  }

  private Callable<Void> generateCorrelationComputeTask(
      String instrument1,
      String instrument2,
      List<Integer> dates,
      Table<String, String, Double> correlationMatrix) {
    return () -> {
      Optional<Double> correlation = computeCorrelation(instrument1, instrument2, dates);
      if (correlation.isPresent()) {
        correlationMatrix.put(instrument1, instrument2, correlation.get());
        correlationMatrix.put(instrument2, instrument1, correlation.get());
      } else {
        log.error("Failed to compute correlation for {} x {}", instrument1, instrument2);
      }
      return null;
    };
  }

  private record ImntDatesRecord(String imnt, int startDate, int endDate, int dateCounts) {}
}
