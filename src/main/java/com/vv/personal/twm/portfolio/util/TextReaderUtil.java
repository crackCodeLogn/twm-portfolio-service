package com.vv.personal.twm.portfolio.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Vivek
 * @since 2024-12-07
 */
public final class TextReaderUtil {
  private TextReaderUtil() {}

  public static List<String> readLines(String fileLocation) {
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
}
