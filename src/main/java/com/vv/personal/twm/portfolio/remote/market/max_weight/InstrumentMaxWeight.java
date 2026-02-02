package com.vv.personal.twm.portfolio.remote.market.max_weight;

import com.vv.personal.twm.portfolio.cache.KeyInstrumentValueCache;
import com.vv.personal.twm.portfolio.util.CsvDownloaderUtil;
import com.vv.personal.twm.portfolio.util.TextReaderUtil;
import java.io.File;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author Vivek
 * @since 2026-02-01
 */
@Slf4j
@AllArgsConstructor
@Component
public class InstrumentMaxWeight {
  private static final String KEY_IMNT_MAX_WEIGHT = "imnt-max-weight";

  private final KeyInstrumentValueCache keyInstrumentValueCache;

  public boolean load(String imntMaxWeightFileLocation) {
    String imntMaxWeightCsvLocation =
        TextReaderUtil.readTextLines(imntMaxWeightFileLocation).get(0);
    String downloadedLocation = CsvDownloaderUtil.downloadCsv(imntMaxWeightCsvLocation);
    return loadFromFile(downloadedLocation);
  }

  public boolean loadFromFile(String downloadedLocation) {
    File file = new File(downloadedLocation);
    if (!file.exists()) {
      log.error("Did not find imnt max weight file: {}, cannot load.", downloadedLocation);
      return false;
    }

    List<String> lines = TextReaderUtil.readTextLines(file.getAbsolutePath());
    keyInstrumentValueCache.setFlushForKey(KEY_IMNT_MAX_WEIGHT, false);
    for (String line : lines) {
      line = line.strip();
      if (line.isBlank()) continue;

      String[] parts = line.split(",");
      String symbol = parts[0];
      Double maxWeight = Double.valueOf(parts[1]);

      keyInstrumentValueCache.offer(KEY_IMNT_MAX_WEIGHT, symbol, maxWeight);
    }
    return true;
  }

  public Optional<Double> getMaxWeight(String imnt) {
    return keyInstrumentValueCache.get(KEY_IMNT_MAX_WEIGHT, imnt);
  }
}
