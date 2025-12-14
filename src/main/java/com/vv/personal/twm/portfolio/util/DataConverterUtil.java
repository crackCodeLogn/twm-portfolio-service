package com.vv.personal.twm.portfolio.util;

import com.google.common.collect.Table;
import com.opencsv.CSVParser;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Vivek
 * @since 2025-12-14
 */
@Slf4j
public final class DataConverterUtil {
  private static final CSVParser parser = new CSVParser();
  private static final List<String> EMPTY_LIST = Collections.emptyList();

  private DataConverterUtil() {}

  public static List<String> split(String data) {
    if (StringUtils.isEmpty(data)) return EMPTY_LIST;
    try {
      return Arrays.stream(parser.parseLine(data)).map(String::strip).toList();
    } catch (IOException e) {
      return EMPTY_LIST;
    }
  }

  public static MarketDataProto.CorrelationMatrix getCorrelationMatrix(
      Optional<Table<String, String, Double>> optionalCorrelationMatrix) {
    if (optionalCorrelationMatrix.isEmpty()) {
      log.warn("Supplied optional correlation matrix is empty");
      return MarketDataProto.CorrelationMatrix.newBuilder().build();
    }
    Table<String, String, Double> correlationMatrix = optionalCorrelationMatrix.get();

    List<MarketDataProto.CorrelationCell> correlationCells =
        new ArrayList<>(correlationMatrix.size());
    correlationMatrix
        .rowMap()
        .forEach(
            (imntRow, imntColValMap) ->
                imntColValMap.forEach(
                    (imntCol, imntValMap) ->
                        correlationCells.add(
                            generateCorrelationCell(imntRow, imntCol, imntValMap))));
    return MarketDataProto.CorrelationMatrix.newBuilder().addAllEntries(correlationCells).build();
  }

  private static MarketDataProto.CorrelationCell generateCorrelationCell(
      String rowKey, String columnKey, Double value) {
    return MarketDataProto.CorrelationCell.newBuilder()
        .setImntRow(rowKey)
        .setImntCol(columnKey)
        .setValue(value)
        .build();
  }
}
