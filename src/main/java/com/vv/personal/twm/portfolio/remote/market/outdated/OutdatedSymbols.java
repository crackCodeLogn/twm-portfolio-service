package com.vv.personal.twm.portfolio.remote.market.outdated;

import com.vv.personal.twm.portfolio.model.market.OutdatedSymbol;
import com.vv.personal.twm.portfolio.util.CsvDownloaderUtil;
import com.vv.personal.twm.portfolio.util.TextReaderUtil;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Vivek
 * @since 2024-12-06
 */
@Slf4j
public class OutdatedSymbols {
  private static final int START_DAY_OF_2XXX = 20000101;
  private static final int LAST_DAY_OF_2XXX = 29991231;

  private final Map<String, OutdatedSymbol> outdatedSymbols = new ConcurrentHashMap<>();

  // TODO - update logic to handle multiple outdated dates for a single symbol, to handle ^VIX

  public boolean load(String outdatedSymbolsFileLocation) {
    String outdatedSymbolCsvLocation =
        TextReaderUtil.readTextLines(outdatedSymbolsFileLocation).get(0);
    String downloadedLocation = CsvDownloaderUtil.downloadCsv(outdatedSymbolCsvLocation);

    File file = new File(downloadedLocation);
    if (!file.exists()) {
      log.error(
          "Did not find outdated symbols file: {}, cannot load outdated symbols.",
          outdatedSymbolsFileLocation);
      return false;
    }

    List<String> lines = TextReaderUtil.readTextLines(file.getAbsolutePath());
    for (String line : lines) {
      line = line.strip();
      if (line.isBlank()) continue;

      String[] parts = line.split(",");
      String symbol = parts[0];
      int outdateStartDate = Integer.parseInt(parts[1]);
      int outdateEndDate = Integer.parseInt(parts[2]);
      outdatedSymbols.computeIfAbsent(
          symbol,
          k ->
              new OutdatedSymbol(
                  symbol,
                  outdateStartDate == -1 ? START_DAY_OF_2XXX : outdateStartDate,
                  outdateEndDate == -1 ? LAST_DAY_OF_2XXX : outdateEndDate));
    }

    return true;
  }

  public boolean contains(String symbol) {
    return outdatedSymbols.containsKey(symbol);
  }

  public Optional<OutdatedSymbol> get(String symbol) {
    return Optional.ofNullable(outdatedSymbols.get(symbol));
  }

  public boolean isEmpty() {
    return outdatedSymbols.isEmpty();
  }

  public boolean isCurrentDateOutdated(String imnt, int date) {
    if (contains(imnt)) {
      Optional<OutdatedSymbol> outdatedSymbol = get(imnt);
      return outdatedSymbol
          .filter(symbol -> date >= symbol.outdateStartDate() && date <= symbol.outdateEndDate())
          .isPresent();
    }
    return false;
  }
}
