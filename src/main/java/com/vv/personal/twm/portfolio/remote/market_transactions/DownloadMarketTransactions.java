package com.vv.personal.twm.portfolio.remote.market_transactions;

import com.google.common.collect.ImmutableMap;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.util.CsvDownloaderUtil;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import com.vv.personal.twm.portfolio.util.TextReaderUtil;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Vivek
 * @since 2024-12-07
 */
@Slf4j
public class DownloadMarketTransactions {

  private static final Map<String, String> countryCodeMap =
      ImmutableMap.<String, String>builder()
          .put("CA", "TO")
          .put("US", "")
          .put("IN", "NS") // nifty-50
          .build();

  private static final Map<String, String> stkExchangeMap =
      ImmutableMap.<String, String>builder().put("ALV", "V").build();

  public static MarketDataProto.Portfolio downloadMarketTransactions(
      String fileLocation, MarketDataProto.Direction direction) {
    MarketDataProto.Portfolio.Builder portfolio = MarketDataProto.Portfolio.newBuilder();

    List<String> lines = TextReaderUtil.readLines(fileLocation);
    if (lines.isEmpty()) {
      log.error("Empty file {}", fileLocation);
      return portfolio.build();
    }

    String link = lines.get(0).strip();
    String downloadLocation = CsvDownloaderUtil.downloadCsv(link);
    if (StringUtils.isEmpty(downloadLocation)) {
      log.error("Empty / Missing download location {}", link);
      return portfolio.build();
    }

    List<String> transactionsLines = TextReaderUtil.readLines(downloadLocation);
    if (transactionsLines.isEmpty()) {
      log.error("Empty / Missing transactions file {}", downloadLocation);
      return portfolio.build();
    }

    return extractMarketTransactions(portfolio, transactionsLines, direction);
  }

  private static String getSymbol(String imntCode, String countryCode) {
    String tickerExtension =
        stkExchangeMap.containsKey(imntCode)
            ? stkExchangeMap.get(imntCode)
            : countryCodeMap.getOrDefault(countryCode, "");
    return String.format("%s.%s", imntCode, tickerExtension);
  }

  private static MarketDataProto.Portfolio extractMarketTransactions(
      MarketDataProto.Portfolio.Builder portfolio,
      List<String> transactionsLines,
      MarketDataProto.Direction direction) {

    for (int i = 1; i < transactionsLines.size(); i++) { // skipping the first line as header
      String line = transactionsLines.get(i);
      String[] parts = line.split(",");
      if (parts.length < 14) {
        log.warn("Failed to parse market transaction {}", line);
        continue;
      }

      String imntCode = parts[0];
      double qty = Double.parseDouble(parts[1]);
      double price = Double.parseDouble(parts[2]);
      double pricerPerShare = Double.parseDouble(parts[3]);
      int tradeDate = DateFormatUtil.getDate(parts[4]);
      int settlementDate = DateFormatUtil.getDate(parts[5]);
      String sector = parts[6];
      MarketDataProto.AccountType accountType =
          MarketDataProto.AccountType.valueOf(parts[7].strip().toUpperCase());
      String orderId = parts[8];
      String cusip = parts[9];
      String accountNumber = parts[10];
      String transactionType = parts[11].toLowerCase();
      String imntName = parts[12];
      String countryCode = parts[13];
      String symbol = getSymbol(imntCode, countryCode);

      MarketDataProto.Value value =
          MarketDataProto.Value.newBuilder().setDate(tradeDate).setPrice(price).build();
      MarketDataProto.Ticker ticker =
          MarketDataProto.Ticker.newBuilder()
              .setSymbol(symbol)
              .setName(imntName)
              .setSector(sector)
              .setType(determineImntType(sector))
              .addData(value)
              .build();
      MarketDataProto.Instrument instrument =
          MarketDataProto.Instrument.newBuilder()
              .setTicker(ticker)
              .setQty(qty)
              .setAccountType(accountType)
              .setDirection(direction)
              .putMetaData("pricePerShare", String.valueOf(pricerPerShare))
              .putMetaData("settlementDate", String.valueOf(settlementDate))
              .putMetaData("orderId", orderId)
              .putMetaData("cusip", cusip)
              .putMetaData("accountNumber", accountNumber)
              .putMetaData("transactionType", transactionType)
              .putMetaData("countryCode", countryCode)
              .build();
      portfolio.addInstruments(instrument);
    }

    return portfolio.build();
  }

  private static MarketDataProto.InstrumentType determineImntType(String sector) {
    sector = sector.toLowerCase();
    if (sector.contains("etf")) return MarketDataProto.InstrumentType.ETF;
    return MarketDataProto.InstrumentType.EQUITY; // improve later if more types come in
  }

  public static void main(String[] args) {
    MarketDataProto.Portfolio portfolio =
        downloadMarketTransactions("/var/mkt-data-b.txt", MarketDataProto.Direction.BUY);
    System.out.println(portfolio);
  }
}
