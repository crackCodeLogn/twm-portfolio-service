package com.vv.personal.twm.portfolio.service.impl;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.vv.personal.twm.portfolio.service.ComputeStatisticsService;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.math.StatisticsUtil;
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
public class ComputeStatisticsServiceImpl implements ComputeStatisticsService {

  private final TickerDataWarehouseService tickerDataWarehouseService;
  private final Map<ImntDatesRecord, Optional<List<Double>>> tmpImntDatesRecordListMap =
      new ConcurrentHashMap<>();

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

    Optional<List<Double>> imnt1Values = Optional.empty();
    Optional<List<Double>> imnt2Values = Optional.empty();

    log.info(
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
      log.info("Correlation between {} x {}: {}", instrument1, instrument2, correlation);
    } catch (Exception e) {
      log.error(
          "Failed to compute correlation between instruments {} x {} correctly",
          instrument1,
          instrument2,
          e);
    } finally {
      timer.stop();
      log.info(
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
        for (int j = i + 1; j < instruments.size(); j++)
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
