package com.vv.personal.twm.portfolio.remote.market.transactions;

import static com.vv.personal.twm.portfolio.util.SanitizerUtil.*;

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

  private static final Map<String, String> customTickerSymbolMap =
      ImmutableMap.<String, String>builder().put("CRT.UN", "CRT-UN").build();

  private static final Map<String, String> stkExchangeMap =
      ImmutableMap.<String, String>builder().put("ALV", "V").build();

  public static MarketDataProto.Portfolio downloadMarketTransactions(
      String fileLocation, MarketDataProto.Direction direction) {
    MarketDataProto.Portfolio.Builder portfolio = MarketDataProto.Portfolio.newBuilder();

    List<String> lines = TextReaderUtil.readTextLines(fileLocation);
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

    List<List<String>> transactionsLines = TextReaderUtil.readCsvLines(downloadLocation);
    if (transactionsLines.isEmpty()) {
      log.error("Empty / Missing transactions file {}", downloadLocation);
      return portfolio.build();
    }

    return extractMarketTransactions(portfolio, transactionsLines, direction);
  }

  public static MarketDataProto.Portfolio downloadMarketDividendTransactions(
      String fileLocation, MarketDataProto.AccountType accountType) {
    MarketDataProto.Portfolio.Builder portfolio = MarketDataProto.Portfolio.newBuilder();

    List<String> lines = TextReaderUtil.readTextLines(fileLocation);
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

    List<List<String>> transactionsLines = TextReaderUtil.readCsvLines(downloadLocation);
    if (transactionsLines.isEmpty()) {
      log.error("Empty / Missing dividends transactions file {}", downloadLocation);
      return portfolio.build();
    }

    return extractMarketTransactions(portfolio, transactionsLines, accountType);
  }

  private static MarketDataProto.Portfolio extractMarketTransactions(
      MarketDataProto.Portfolio.Builder portfolio,
      List<List<String>> transactionsLines,
      MarketDataProto.Direction direction) {

    for (int i = 1; i < transactionsLines.size(); i++) { // skipping the first line as header
      List<String> parts = transactionsLines.get(i);
      if (parts.size() < 14) {
        log.warn("Failed to parse market transaction {}", parts);
        continue;
      }

      String imntCode = parts.get(0);
      if (imntCode.isBlank()) continue;

      double qty = Double.parseDouble(parts.get(1));
      double price = Double.parseDouble(parts.get(2));
      double pricerPerShare = Double.parseDouble(parts.get(3));
      int tradeDate = DateFormatUtil.getDate(parts.get(4));
      int settlementDate = DateFormatUtil.getDate(parts.get(5));
      String sector = parts.get(6);
      MarketDataProto.AccountType accountType =
          MarketDataProto.AccountType.valueOf(parts.get(7).strip().toUpperCase());
      String orderId = parts.get(8);
      String cusip = parts.get(9);
      String accountNumber = parts.get(10);
      String transactionType = parts.get(11).toLowerCase();
      String imntName = parts.get(12);
      String countryCode = parts.get(13);
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

  private static MarketDataProto.Portfolio extractMarketTransactions(
      MarketDataProto.Portfolio.Builder portfolio,
      List<List<String>> transactionsLines,
      MarketDataProto.AccountType accountType) {
    if (accountType == MarketDataProto.AccountType.NR)
      return extractNrDividends(portfolio, transactionsLines);
    else if (accountType == MarketDataProto.AccountType.TFSA)
      return extractTfsaDividends(portfolio, transactionsLines);
    else if (accountType == MarketDataProto.AccountType.FHSA)
      return extractFhsaDividends(portfolio, transactionsLines);

    return portfolio.build();
  }

  private static MarketDataProto.Portfolio extractNrDividends(
      MarketDataProto.Portfolio.Builder portfolio, List<List<String>> transactionsLines) {

    for (int i = 1; i < transactionsLines.size(); i++) { // skipping the first line as header
      List<String> parts = transactionsLines.get(i);
      if (parts.size() < 4) {
        log.warn("Failed to parse nr dividend transaction {}", parts);
        continue;
      }

      String date = sanitizeString(parts.get(0));
      double dividend = sanitizeDouble(parts.get(1));
      String purposeTicker = sanitizeString(parts.get(2));
      String type = sanitizeString(parts.get(3)).toLowerCase();
      boolean isManufactured = purposeTicker.endsWith("-m");
      if (!"div".equals(type)) continue;
      if (isManufactured) purposeTicker = purposeTicker.substring(0, purposeTicker.length() - 2);

      String symbol = getSymbol(purposeTicker, "CA"); // nr is strictly canadian at this point
      int divDate = DateFormatUtil.getDate(date);

      MarketDataProto.Instrument instrument =
          generateDividendInstrument(
              symbol, divDate, dividend, MarketDataProto.AccountType.NR, isManufactured);
      portfolio.addInstruments(instrument);
    }
    return portfolio.build();
  }

  private static MarketDataProto.Portfolio extractTfsaDividends(
      MarketDataProto.Portfolio.Builder portfolio, List<List<String>> transactionsLines) {
    for (int i = 1; i < transactionsLines.size(); i++) { // skipping the first line as header
      List<String> parts = transactionsLines.get(i);
      if (parts.size() < 7) {
        log.warn("Failed to parse tfsa dividend transaction {}", parts);
        continue;
      }

      String date = sanitizeString(parts.get(0));
      double dividend = sanitizeDouble(parts.get(1));
      String purposeTicker = sanitizeString(parts.get(5));
      String type = sanitizeString(parts.get(6)).toLowerCase();
      boolean isManufactured = purposeTicker.endsWith("-m");
      if (!"div".equals(type)) continue;
      if (isManufactured) purposeTicker = purposeTicker.substring(0, purposeTicker.length() - 2);

      String symbol = getSymbol(purposeTicker, "CA"); // nr is strictly canadian at this point
      int divDate = DateFormatUtil.getDate(date);

      MarketDataProto.Instrument instrument =
          generateDividendInstrument(
              symbol, divDate, dividend, MarketDataProto.AccountType.TFSA, isManufactured);
      portfolio.addInstruments(instrument);
    }
    return portfolio.build();
  }

  private static MarketDataProto.Portfolio extractFhsaDividends(
      MarketDataProto.Portfolio.Builder portfolio, List<List<String>> transactionsLines) {
    for (int i = 1; i < transactionsLines.size(); i++) { // skipping the first line as header
      List<String> parts = transactionsLines.get(i);
      if (parts.size() < 7) {
        log.warn("Failed to parse fhsa dividend transaction {}", parts);
        continue;
      }

      String date = sanitizeString(parts.get(0));
      double dividend = sanitizeDouble(parts.get(1));
      String purposeTicker = sanitizeString(parts.get(5));
      String type = sanitizeString(parts.get(6)).toLowerCase();
      boolean isManufactured = purposeTicker.endsWith("-m");
      if (!"div".equals(type)) continue;
      if (isManufactured) purposeTicker = purposeTicker.substring(0, purposeTicker.length() - 2);

      String symbol = getSymbol(purposeTicker, "CA"); // nr is strictly canadian at this point
      int divDate = DateFormatUtil.getDate(date);

      MarketDataProto.Instrument instrument =
          generateDividendInstrument(
              symbol, divDate, dividend, MarketDataProto.AccountType.FHSA, isManufactured);
      portfolio.addInstruments(instrument);
    }
    return portfolio.build();
  }

  private static MarketDataProto.Instrument generateDividendInstrument(
      String symbol,
      int date,
      double dividend,
      MarketDataProto.AccountType accountType,
      boolean isManufactured) {
    return MarketDataProto.Instrument.newBuilder()
        .setAccountType(accountType)
        .setTicker(
            MarketDataProto.Ticker.newBuilder()
                .setSymbol(symbol)
                .addData(
                    MarketDataProto.Value.newBuilder().setDate(date).setPrice(dividend).build())
                .build())
        .putMetaData("orderId", generateDividendOrderId(symbol, date, accountType, isManufactured))
        .putMetaData("isManufactured", String.valueOf(isManufactured))
        .build();
  }

  private static String generateDividendOrderId(
      String symbol, int date, MarketDataProto.AccountType accountType, boolean isManufactured) {
    return String.format("DIVIDEND_%d-%s-%d-%s", date, accountType, isManufactured ? 1 : 0, symbol);
  }

  private static String getSymbol(String imntCode, String countryCode) {
    imntCode = imntCode.strip();
    // custom ticker symbol mapping
    if (customTickerSymbolMap.containsKey(imntCode)) {
      imntCode = customTickerSymbolMap.get(imntCode);
    }

    String tickerExtension =
        stkExchangeMap.containsKey(imntCode)
            ? stkExchangeMap.get(imntCode)
            : countryCodeMap.getOrDefault(countryCode, "");
    return String.format("%s.%s", imntCode, tickerExtension);
  }

  private static MarketDataProto.InstrumentType determineImntType(String sector) {
    sector = sector.toLowerCase();
    if (sector.contains("etf")) return MarketDataProto.InstrumentType.ETF;
    return MarketDataProto.InstrumentType.EQUITY; // improve later if more types come in
  }

  public static void main(String[] args) {
    MarketDataProto.Portfolio portfolio;
    portfolio = downloadMarketTransactions("/var/mkt-data-b.txt", MarketDataProto.Direction.BUY);
    System.out.println(portfolio);

    portfolio =
        downloadMarketDividendTransactions(
            "/var/mkt-data-div-t.txt", MarketDataProto.AccountType.TFSA);
    System.out.println(portfolio);
    double totalDividend = 0.0;
    for (MarketDataProto.Instrument instrument : portfolio.getInstrumentsList()) {
      totalDividend += instrument.getTicker().getData(0).getPrice();
    }
    System.out.println("totalDividend for t => $" + totalDividend);

    portfolio =
        downloadMarketDividendTransactions(
            "/var/mkt-data-div-n.txt", MarketDataProto.AccountType.NR);
    System.out.println(portfolio);
    totalDividend = 0.0;
    for (MarketDataProto.Instrument instrument : portfolio.getInstrumentsList()) {
      totalDividend += instrument.getTicker().getData(0).getPrice();
    }
    System.out.println("totalDividend for n => $" + totalDividend);
  }
}
