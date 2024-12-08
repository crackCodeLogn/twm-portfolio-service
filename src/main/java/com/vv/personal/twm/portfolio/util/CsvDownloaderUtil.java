package com.vv.personal.twm.portfolio.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

/**
 * @author Vivek
 * @since 2024-12-07
 */
@Slf4j
public final class CsvDownloaderUtil {
  private CsvDownloaderUtil() {}

  public static String downloadCsv(String csvUrlLocation) {
    return downloadCsv(
        csvUrlLocation, "/tmp", String.valueOf(Math.abs(Objects.hash(csvUrlLocation))));
  }

  public static String downloadCsv(
      String csvUrlLocation, String destinationFolder, String destinationFileName) {
    URL url;

    try {
      url = new URL(csvUrlLocation);
    } catch (MalformedURLException e) {
      return "";
    }

    File file = new File(String.format("%s/%s.csv", destinationFolder, destinationFileName));
    try {
      log.info("Downloading csv from {}", csvUrlLocation);
      FileUtils.copyURLToFile(url, file);
      log.info("Download completed and stored at {}", file.getAbsolutePath());
      return file.getAbsolutePath();
    } catch (IOException e) {
      log.error(e.getMessage());
    }
    return "";
  }
}
