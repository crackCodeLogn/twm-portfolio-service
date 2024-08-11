package com.vv.personal.twm.portfolio.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * @author Vivek
 * @since 2023-11-24
 */
public final class DateFormatUtil {
    private DateFormatUtil() {
    }

    public static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("[MM/dd/yyyy][M/dd/yyyy][MM/d/yyyy][M/d/yyyy][yyyy-MM-dd]")
            .toFormatter();

    public static LocalDate getLocalDate(String date) {
        return LocalDate.parse(date, DATE_TIME_FORMATTER);
    }

}
