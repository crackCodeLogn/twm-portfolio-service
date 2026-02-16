package com.vv.personal.twm.portfolio.remote.market.outdated;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.model.market.OutdatedSymbol;
import com.vv.personal.twm.portfolio.util.CsvDownloaderUtil;
import com.vv.personal.twm.portfolio.util.TextReaderUtil;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
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

  private final Map<String, TreeSet<OutdatedSymbol>> outdatedSymbols = new ConcurrentHashMap<>();

  public boolean load(String outdatedSymbolsFileLocation) {
    String outdatedSymbolCsvLocation =
        TextReaderUtil.readTextLines(outdatedSymbolsFileLocation).get(0);
    String downloadedLocation = CsvDownloaderUtil.downloadCsv(outdatedSymbolCsvLocation);
    return loadFromFile(downloadedLocation);
  }

  public boolean loadFromFile(String downloadedLocation) {
    File file = new File(downloadedLocation);
    if (!file.exists()) {
      log.error(
          "Did not find outdated symbols file: {}, cannot load outdated symbols.",
          downloadedLocation);
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
      outdatedSymbols.computeIfAbsent(symbol, k -> new TreeSet<>());
      outdatedSymbols.get(symbol).add(generateOutdatedSymbol(outdateStartDate, outdateEndDate));
    }

    return true;
  }

  /** log(n) look up */
  public boolean isCurrentDateOutdated(String imnt, int date) {
    if (contains(imnt)) {
      Optional<TreeSet<OutdatedSymbol>> outdatedSymbol = get(imnt);
      if (outdatedSymbol.isPresent()) {
        OutdatedSymbol inputDate = generateOutdatedSymbol(date, date);
        OutdatedSymbol base = outdatedSymbol.get().floor(inputDate);
        if (base == null) return false;
        return date >= base.outdateStartDate() && date <= base.outdateEndDate();
      }
    }
    return false;
  }

  public boolean isDelisted(String imnt) {
    return get(imnt)
        .filter(symbols -> symbols.last().outdateEndDate() == LAST_DAY_OF_2XXX)
        .isPresent();
  }

  public List<MarketDataProto.Instrument> getDelistedSymbols() {
    return outdatedSymbols.keySet().stream()
        .filter(this::isDelisted)
        .map(
            symbol ->
                MarketDataProto.Instrument.newBuilder()
                    .setTicker(
                        MarketDataProto.Ticker.newBuilder()
                            .setSymbol(String.format(symbol))
                            .setName(String.format("[D] %s", symbol))
                            .build())
                    .build())
        .toList();
  }

  public boolean contains(String symbol) {
    return outdatedSymbols.containsKey(symbol);
  }

  public Optional<TreeSet<OutdatedSymbol>> get(String symbol) {
    return Optional.ofNullable(outdatedSymbols.get(symbol));
  }

  private OutdatedSymbol generateOutdatedSymbol(int startDate, int endDate) {
    return new OutdatedSymbol(
        startDate == -1 ? START_DAY_OF_2XXX : startDate,
        endDate == -1 ? LAST_DAY_OF_2XXX : endDate);
  }
}
