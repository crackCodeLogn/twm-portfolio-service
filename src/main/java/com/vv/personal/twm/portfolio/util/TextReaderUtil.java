package com.vv.personal.twm.portfolio.util;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Vivek
 * @since 2024-12-07
 */
public final class TextReaderUtil {
  private TextReaderUtil() {}

  public static List<String> readTextLines(String fileLocation) {
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(fileLocation))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return lines;
  }

  public static List<List<String>> readCsvLines(String fileLocation) {
    return readCsvLines(fileLocation, ',');
  }

  public static List<List<String>> readCsvLines(String fileLocation, char delimiter) {
    List<List<String>> lines = new ArrayList<>();
    try (CSVReader reader =
        new CSVReaderBuilder(new FileReader(fileLocation))
            .withCSVParser(new CSVParserBuilder().withSeparator(delimiter).build())
            .build()) {
      List<String[]> csvLines = reader.readAll();
      for (String[] csvLine : csvLines) {
        lines.add(Arrays.stream(csvLine).toList());
      }

    } catch (IOException | CsvException e) {
      e.printStackTrace();
    }
    return lines;
  }
}
