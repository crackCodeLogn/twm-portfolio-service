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

  private final Map<String, OutdatedSymbol> outdatedSymbols = new ConcurrentHashMap<>();

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
      int lastListingDate = Integer.parseInt(parts[1]);
      outdatedSymbols.computeIfAbsent(symbol, k -> new OutdatedSymbol(symbol, lastListingDate));
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
}
