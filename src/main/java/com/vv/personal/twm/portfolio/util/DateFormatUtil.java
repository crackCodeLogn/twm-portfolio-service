package com.vv.personal.twm.portfolio.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * @author Vivek
 * @since 2023-11-24
 */
public final class DateFormatUtil {
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("[MM/dd/yyyy][M/dd/yyyy][MM/d/yyyy][M/d/yyyy][yyyy-MM-dd][yyyyMMdd]")
          .toFormatter();

  private DateFormatUtil() {}

  public static LocalDate getLocalDate(String date) {
    return LocalDate.parse(date, DATE_TIME_FORMATTER);
  }

  public static LocalDate getLocalDate(int date) {
    return getLocalDate(String.valueOf(date));
  }

  public static int getLocalDate(LocalDate date) {
    return date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
  }
}
