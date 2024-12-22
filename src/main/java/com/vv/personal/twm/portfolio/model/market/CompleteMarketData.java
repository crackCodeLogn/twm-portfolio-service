package com.vv.personal.twm.portfolio.model.market;

import static com.vv.personal.twm.portfolio.util.SanitizerUtil.sanitizeDouble;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.OutdatedSymbols;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

/**
 * @author Vivek
 * @since 2024-09-13
 */
@Slf4j
@Getter
@Setter
public class CompleteMarketData {

  private static final OutdatedSymbol notFoundOutdatedSymbol =
      new OutdatedSymbol("dummy", 29991231);

  // Holds map of ticker x (map of account type x doubly linked list nodes of transactions done)
  private final Map<String, Map<MarketDataProto.AccountType, DataList>> marketData;
  private final Map<String, Map<MarketDataProto.AccountType, Map<Integer, DividendRecord>>>
      imntDividendsMap;
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      dateDividendsMap; // date x account type x divs for that date

  // post processes, i.e. not filled during startup
  // todo - think about filling all the date based maps with 0s based off entire dates
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      realizedDatePnLMap; // pure date x account type x sells
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      unrealizedDatePnLMap; // pure date x account type x unrealized
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>>
      combinedDatePnLMap; // combined pure date x account type x (realized + unrealized)
  private final Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>>
      unrealizedImntPnLMap; // pure imnt x account type x date x unrealized
  private final Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>>
      realizedImntPnLMap; // pure imnt sells
  private final TreeMap<Integer, Map<MarketDataProto.AccountType, Double>>
      realizedWithDividendDatePnLMap; // cumulative date x account type x (sells + divs)
  private final Map<String, Map<MarketDataProto.AccountType, TreeMap<Integer, Double>>>
      realizedImntWithDividendPnLMap; // cumulative imnt x account type x date x (sells + divs)
  private final TreeMap<Integer, Map<MarketDataProto.AccountType, Double>>
      dateDividendsCumulativeMap; // cumulative date x account type x divs
  private final TreeMap<Integer, Map<MarketDataProto.AccountType, Double>>
      combinedDatePnLCumulativeMap; // cumulative date x account type x (unrealized + sells + divs)
  // SPECIAL NOTE: combinedDatePnLCumulativeMap does not include div-only non-market dates
  private final Map<LocalDate, Integer> localDateAndDateMap;
  private final Map<Integer, LocalDate> dateAndLocalDateMap;
  private TickerDataWarehouseService tickerDataWarehouseService;
  private OutdatedSymbols outdatedSymbols;

  public CompleteMarketData() {
    marketData = new ConcurrentHashMap<>();
    imntDividendsMap = new ConcurrentHashMap<>();
    dateDividendsMap = new ConcurrentHashMap<>();
    realizedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    unrealizedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    combinedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    unrealizedImntPnLMap = new ConcurrentHashMap<>();
    realizedImntPnLMap = new ConcurrentHashMap<>();
    realizedWithDividendDatePnLMap = new TreeMap<>();
    realizedImntWithDividendPnLMap = new ConcurrentHashMap<>();
    dateDividendsCumulativeMap = new TreeMap<>();
    combinedDatePnLCumulativeMap = new TreeMap<>();

    tickerDataWarehouseService = null;
    localDateAndDateMap = new ConcurrentHashMap<>();
    dateAndLocalDateMap = new ConcurrentHashMap<>();
  }

  public void populate(MarketDataProto.Portfolio portfolio) {
    portfolio
        .getInstrumentsList()
        .forEach(
            instrument -> {
              marketData.computeIfAbsent(instrument.getTicker().getSymbol(), k -> new HashMap<>());

              Map<MarketDataProto.AccountType, DataList> dataListMap =
                  marketData.get(instrument.getTicker().getSymbol());
              dataListMap.computeIfAbsent(instrument.getAccountType(), k -> new DataList());

              dataListMap.get(instrument.getAccountType()).addBlock(instrument);
            });
  }

  public void populateDividends(MarketDataProto.Portfolio portfolio) {
    dateDividendsCumulativeMap.put(0, new HashMap<>());
    List<MarketDataProto.AccountType> accountTypes = getAccountTypes();
    accountTypes.forEach(
        type -> {
          dateDividendsCumulativeMap.get(0).put(type, 0.0);
        }); // base line

    log.info("Beginning dividends population.");
    StopWatch stopWatch = StopWatch.createStarted();

    for (MarketDataProto.Instrument instrument : portfolio.getInstrumentsList()) {
      String orderId = instrument.getMetaDataOrDefault("orderId", "");
      if (orderId.isEmpty()) {
        log.error("orderId is empty for dividend transaction: {}", instrument);
        continue;
      }

      int divDate = instrument.getTicker().getData(0).getDate();
      double dividend = instrument.getTicker().getData(0).getPrice();
      MarketDataProto.AccountType accountType = instrument.getAccountType();

      // record dividend data in imntDividendsMap as the data structure holding translated data from
      // the div list
      imntDividendsMap.computeIfAbsent(instrument.getTicker().getSymbol(), k -> new HashMap<>());
      Map<MarketDataProto.AccountType, Map<Integer, DividendRecord>> divDateValueMap =
          imntDividendsMap.get(instrument.getTicker().getSymbol());
      divDateValueMap.computeIfAbsent(accountType, k -> new HashMap<>());
      divDateValueMap
          .get(accountType)
          .put(
              divDate,
              new DividendRecord(instrument.getTicker().getSymbol(), divDate, dividend, orderId));

      // equivalent of above structure but an agg based on date and irrespective of imnt
      Map<MarketDataProto.AccountType, Double> dateDivAccountTypeDivMap =
          dateDividendsMap.computeIfAbsent(divDate, k -> new HashMap<>());
      dateDivAccountTypeDivMap.putIfAbsent(accountType, 0.0);
      dateDivAccountTypeDivMap.compute(accountType, (k, v) -> sanitizeDouble(v) + dividend);
    }

    // operate below for accumulation
    // note: do not add the date / local date to the instance localDateAndDateMap or
    // dateAndLocalDateMap as the absence is used to calculate missing div dates in computePnL
    List<Integer> divDates = new ArrayList<>(dateDividendsMap.keySet());
    Collections.sort(divDates);
    Map<MarketDataProto.AccountType, Double> cumulativeDivs = new HashMap<>();
    accountTypes.forEach(accountType -> cumulativeDivs.put(accountType, 0.0));
    for (int i = 0; i < divDates.size(); i++) { // skip 0th as that is date = 0
      int date = divDates.get(i);
      Map<MarketDataProto.AccountType, Double> typeDivBaseMap = dateDividendsMap.get(date);
      Map<MarketDataProto.AccountType, Double> typeDivTargetMap =
          dateDividendsCumulativeMap.computeIfAbsent(date, k -> new HashMap<>());
      accountTypes.forEach(
          accountType -> typeDivTargetMap.put(accountType, cumulativeDivs.get(accountType)));

      typeDivBaseMap.forEach(
          (type, div) -> {
            typeDivTargetMap.compute(type, (k, v) -> sanitizeDouble(v) + div);
            cumulativeDivs.put(type, cumulativeDivs.getOrDefault(type, 0.0) + div);
          });
    }
    /*
    LocalDate localDate = DateFormatUtil.getLocalDate(divDate);
    int dateTMinus1 = DateFormatUtil.getDate(localDate.minusDays(1));

    Map<MarketDataProto.AccountType, Double> typeDividendCumulativeMap =
        dateDividendsCumulativeMap.computeIfAbsent(divDate, k -> new HashMap<>());
    // update current date with t-1 data point
    accountTypes.forEach(
        accountType1 -> {
          Double divTMinus1AccountTypeDividend =
              dateDividendsCumulativeMap.floorEntry(dateTMinus1).getValue().get(accountType1);
          typeDividendCumulativeMap.putIfAbsent(accountType1, 0.0);
          typeDividendCumulativeMap.compute(
              accountType1, (k, v) -> sanitizeDouble(v) + divTMinus1AccountTypeDividend);
        });

    // update current date with t data point
    typeDividendCumulativeMap.compute(accountType, (k, v) -> sanitizeDouble(v) + dividend);*/

    stopWatch.stop();
    log.info(
        "Dividend calculation and population completed in {} ms",
        stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  public void computeAcb() {
    marketData.values().forEach(collection -> collection.values().forEach(DataList::computeAcb));
  }

  public void computePnL() {
    if (tickerDataWarehouseService == null) {
      log.error("Cannot compute PnL without tickerDataWarehouseService");
    }
    List<LocalDate> dates = tickerDataWarehouseService.getDates();
    dates.forEach(
        date -> {
          int intDate = DateFormatUtil.getDate(date);
          localDateAndDateMap.putIfAbsent(date, intDate);
          dateAndLocalDateMap.putIfAbsent(intDate, date);
        });
    Set<Integer> dividendDates = getDividendDates();
    boolean toSort = false;
    for (Integer dividendDate : dividendDates) {
      if (!dateAndLocalDateMap.containsKey(dividendDate)) {
        log.info("Found a non-market dividend date: {}", dividendDate);
        LocalDate localDate = DateFormatUtil.getLocalDate(dividendDate);
        dateAndLocalDateMap.putIfAbsent(dividendDate, localDate);
        localDateAndDateMap.putIfAbsent(localDate, dividendDate);
        dates.add(localDate);
        toSort = true;
      }
    }
    if (toSort) Collections.sort(dates);

    boolean failed = false;
    outer:
    for (Map.Entry<String, Map<MarketDataProto.AccountType, DataList>> marketDataEntry :
        marketData.entrySet()) { // iterate over the marketData
      String imnt = marketDataEntry.getKey();
      Map<MarketDataProto.AccountType, DataList> typeDataMap = marketDataEntry.getValue();
      for (Map.Entry<MarketDataProto.AccountType, DataList> entry : typeDataMap.entrySet()) {
        MarketDataProto.AccountType type = entry.getKey();
        DataList dataList = entry.getValue();
        DataNode node = dataList.getHead();
        int nodeDate = getDate(node);
        int dateIndex = 0;

        // find the initial date index for the nodeDate
        while (dateIndex < dates.size()
            && localDateAndDateMap.get(dates.get(dateIndex)) != nodeDate) {
          dateIndex++;
        }

        log.info("Found dateIndex: {} for {} of {} {}", dateIndex, nodeDate, imnt, type);
        while (dateIndex < dates.size()) {
          int date = localDateAndDateMap.get(dates.get(dateIndex));
          // find and point to the correct node for the date
          while (node.getNext() != null && getDate(node.getNext()) == date) {
            node = node.getNext();
          }
          // System.out.println(date + " " + node);

          Double marketPrice = tickerDataWarehouseService.getMarketData(imnt, date);
          if (marketPrice == null) {
            if (outdatedSymbols != null
                && outdatedSymbols.contains(imnt)
                && date
                    >= outdatedSymbols.get(imnt).orElse(notFoundOutdatedSymbol).lastListingDate()) {
              log.debug("Allowing skip of market price for outdated {} x {}", imnt, date);
            } else if (dividendDates.contains(date)) {
              log.info("Allowed to miss off-market dividend date: {}", date);
              dateIndex++;
              continue;
            } else {
              log.error(
                  "Did not find market price for {} x {}, CANNOT compute any further!", imnt, date);
              failed = true;
              break outer;
            }
            dateIndex++;
            continue;
          }
          computeUnrealizedPnL(imnt, type, node, date, marketPrice);
          if (node.getInstrument().getDirection() == MarketDataProto.Direction.BUY) {
            dateIndex++;
            continue;
          }
          computeRealizedPnL(
              imnt, type, node, date, marketPrice); // realized pnl (w/o div) changes only on SELL
          dateIndex++;
        }
      }
    }
    /*System.out.println("Spewing out acb data list of vfv.to nr to analyze: "); // todo - remove
    DataNode vfvNrNode = marketData.get("VFV.TO").get(MarketDataProto.AccountType.NR).getHead();
    while (vfvNrNode != null) {
      System.out.printf(
          "%s %f %d %f %s %s %f %f %f\n",
          vfvNrNode.getInstrument().getTicker().getSymbol(),
          vfvNrNode.getInstrument().getQty(),
          vfvNrNode.getInstrument().getTicker().getData(0).getDate(),
          vfvNrNode.getInstrument().getTicker().getData(0).getPrice(),
          vfvNrNode.getInstrument().getMetaDataOrDefault("pricePerShare", "0.0"),
          vfvNrNode.getInstrument().getDirection(),
          vfvNrNode.getRunningQuantity(),
          vfvNrNode.getAcb().getAcbPerUnit(),
          vfvNrNode.getAcb().getTotalAcb());
      vfvNrNode = vfvNrNode.getNext();
    }*/
    unrealizedDatePnLMap.forEach(
        (k, v) -> System.out.printf("%d x %.6f\n", k, v.get(MarketDataProto.AccountType.NR)));

    if (!failed) {
      computeCombinedPnL(dates);

      computeRealizedPnLFromDividends(dates);
      computeCombinedPnLCumulative();
      System.out.println(
          "Combined PnL of TFSA: "
              + combinedDatePnLCumulativeMap
                  .floorEntry(20241220)
                  .getValue()
                  .get(MarketDataProto.AccountType.TFSA));
      System.out.println(
          "Combined PnL of NR: "
              + combinedDatePnLCumulativeMap
                  .floorEntry(20241220)
                  .getValue()
                  .get(MarketDataProto.AccountType.NR));

    } else log.error("Failed to compute pnL. Check logs for relevant error.");
  }

  // inflate realized pnl with dividend data
  private void computeRealizedPnLFromDividends(List<LocalDate> dates) {
    List<MarketDataProto.AccountType> accountTypes = getAccountTypes();
    realizedImntWithDividendPnLMap.clear();
    //    realizedImntWithDividendPnLMap.putAll(realizedImntPnLMap); // doesn't actually create new
    // copies

    // deep copy realizedImntPnLMap -> realizedImntWithDividendPnLMap
    realizedImntPnLMap.forEach(
        (imnt, typeDatePriceMap) ->
            typeDatePriceMap.forEach(
                (type, datePriceMap) ->
                    datePriceMap.forEach(
                        (date, price) -> {
                          Map<MarketDataProto.AccountType, TreeMap<Integer, Double>> imntMap =
                              realizedImntWithDividendPnLMap.computeIfAbsent(
                                  imnt, k -> new HashMap<>());
                          Map<Integer, Double> dtPriceMap =
                              imntMap.computeIfAbsent(type, k -> new TreeMap<>());
                          dtPriceMap.put(date, price);
                        })));
    // deep copy completed

    // populate realizedImntWithDividendPnLMap
    for (String dividendTicker : imntDividendsMap.keySet()) {
      Map<MarketDataProto.AccountType, Map<Integer, DividendRecord>> accountTypeMapMap =
          imntDividendsMap.get(dividendTicker);

      accountTypeMapMap.forEach(
          (accountType, dateRecordMap) ->
              dateRecordMap.forEach(
                  (date, record) -> {
                    // init
                    Map<MarketDataProto.AccountType, TreeMap<Integer, Double>> typeMapMap =
                        realizedImntWithDividendPnLMap.computeIfAbsent(
                            dividendTicker,
                            k2 -> new HashMap<>()); // for CAD manufactured kind of things
                    typeMapMap.computeIfAbsent(accountType, k1 -> new TreeMap<>());
                    typeMapMap.get(accountType).putIfAbsent(0, 0.0); // baseline

                    int dateTMinus1 =
                        DateFormatUtil.getDate(dateAndLocalDateMap.get(date).minusDays(1));
                    Double realizedPnLTMinus1 =
                        typeMapMap.get(accountType).floorEntry(dateTMinus1).getValue();

                    typeMapMap
                        .get(accountType)
                        .compute(
                            date,
                            (k1, v1) ->
                                sanitizeDouble(v1)
                                    + record.dividend()
                                    + sanitizeDouble(realizedPnLTMinus1));
                  }));
    }

    // compute for adding to realizedWithDividendDatePnLMap
    realizedWithDividendDatePnLMap.put(0, new HashMap<>());
    accountTypes.forEach(type -> realizedWithDividendDatePnLMap.get(0).put(type, 0.0)); // base line
    dates.forEach(
        localDate -> {
          int date = localDateAndDateMap.get(localDate);
          realizedWithDividendDatePnLMap.putIfAbsent(date, new HashMap<>());
          Map<MarketDataProto.AccountType, Double> typePnLRealizedWithDivDatePnLMap =
              realizedWithDividendDatePnLMap.get(date);

          accountTypes.forEach(
              accountType -> {
                typePnLRealizedWithDivDatePnLMap.putIfAbsent(accountType, 0.0);

                //                System.out.println(date);
                //                System.out.println(accountType);

                int dateTMinus1 =
                    DateFormatUtil.getDate(dateAndLocalDateMap.get(date).minusDays(1));
                Double realizedTMinus1 =
                    realizedWithDividendDatePnLMap
                        .floorEntry(dateTMinus1)
                        .getValue()
                        .get(accountType);

                Double dividendValue =
                    dateDividendsMap.containsKey(date)
                        ? dateDividendsMap.get(date).get(accountType)
                        : null;
                Double realizedValue =
                    realizedDatePnLMap.containsKey(date)
                        ? realizedDatePnLMap.get(date).get(accountType)
                        : null;

                double totalValue =
                    sanitizeDouble(realizedTMinus1)
                        + sanitizeDouble(dividendValue)
                        + sanitizeDouble(realizedValue);
                typePnLRealizedWithDivDatePnLMap.compute(
                    accountType, (k, v) -> sanitizeDouble(v) + totalValue);
              });
        });
  }

  private Set<Integer> getDividendDates() {
    return dateDividendsMap.keySet();
  }

  private void computeRealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getInstrument().getQty();

    double tickerPrice = node.getPrev().getAcb().getAcbPerUnit();
    double pnL = (marketPrice - tickerPrice) * sellQty;

    if (date == 20241220)
      log.info(
          "realized pnL {} x {} x {} x {} => mkt price '{}', qty '{}', ticker price '{}' => pnl= {}",
          imnt,
          type.name(),
          date,
          node.getInstrument().getDirection().name(),
          marketPrice,
          sellQty,
          tickerPrice,
          pnL);
    realizedDatePnLMap.computeIfAbsent(
        date,
        k -> {
          Map<MarketDataProto.AccountType, Double> typePriceMap = new HashMap<>();
          getAccountTypes().forEach(accountType -> typePriceMap.put(accountType, 0.0));
          return typePriceMap;
        });
    realizedImntPnLMap.computeIfAbsent(
        imnt,
        k -> {
          Map<MarketDataProto.AccountType, Map<Integer, Double>> typeDatePriceMap = new HashMap<>();
          getAccountTypes()
              .forEach(accountType -> typeDatePriceMap.put(accountType, new HashMap<>()));
          return typeDatePriceMap;
        });

    realizedDatePnLMap.get(date).compute(type, (k, v) -> sanitizeDouble(v) + pnL);
    realizedImntPnLMap.get(imnt).get(type).compute(date, (k, v) -> sanitizeDouble(v) + pnL);
  }

  private void computeUnrealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getRunningQuantity();

    double tickerPrice = node.getAcb().getAcbPerUnit();
    double pnL = (marketPrice - tickerPrice) * sellQty;

    if (date == 20241220)
      log.info(
          "unrealized pnL {} x {} x {} x {} => mkt price '{}', qty '{}', ticker price '{}' => pnl= {}",
          imnt,
          type.name(),
          date,
          node.getInstrument().getDirection().name(),
          marketPrice,
          sellQty,
          tickerPrice,
          pnL);
    unrealizedDatePnLMap.computeIfAbsent(
        date,
        k -> {
          Map<MarketDataProto.AccountType, Double> typePriceMap = new HashMap<>();
          getAccountTypes().forEach(accountType -> typePriceMap.put(accountType, 0.0));
          return typePriceMap;
        });
    unrealizedImntPnLMap.computeIfAbsent(
        imnt,
        k -> {
          Map<MarketDataProto.AccountType, Map<Integer, Double>> typeDatePriceMap = new HashMap<>();
          getAccountTypes()
              .forEach(accountType -> typeDatePriceMap.put(accountType, new HashMap<>()));
          return typeDatePriceMap;
        });

    unrealizedDatePnLMap.get(date).compute(type, (k, v) -> sanitizeDouble(v) + pnL);
    unrealizedImntPnLMap.get(imnt).get(type).compute(date, (k, v) -> sanitizeDouble(v) + pnL);
  }

  private void computeCombinedPnL(List<LocalDate> dates) {
    List<MarketDataProto.AccountType> accountTypes = getAccountTypes();

    dates.forEach(
        localDate -> {
          int date = localDateAndDateMap.get(localDate);
          for (MarketDataProto.AccountType type : accountTypes) {
            Double unrealizedPnL = null;
            Double realizedPnL = null;
            if (unrealizedDatePnLMap.containsKey(date)) {
              unrealizedPnL = unrealizedDatePnLMap.get(date).get(type);
            }
            if (realizedDatePnLMap.containsKey(date)) {
              realizedPnL = realizedDatePnLMap.get(date).get(type);
            }
            if (realizedPnL == null && unrealizedPnL == null) {
              continue;
            }
            double combinedPnL = sanitizeDouble(unrealizedPnL) + sanitizeDouble(realizedPnL);

            if (date == 20241220)
              log.info("combined pnL {} x {} => pnl= {}", date, type.name(), combinedPnL);

            combinedDatePnLMap.computeIfAbsent(date, k -> new HashMap<>());
            combinedDatePnLMap.get(date).compute(type, (k, v) -> sanitizeDouble(v) + combinedPnL);
          }
        });
  }

  private void computeCombinedPnLCumulative() {
    combinedDatePnLCumulativeMap.putIfAbsent(0, new HashMap<>());
    getAccountTypes()
        .forEach(accountType -> combinedDatePnLCumulativeMap.get(0).put(accountType, 0.0));

    combinedDatePnLMap.forEach(
        (date, accountTypePnLMap) -> {
          accountTypePnLMap.forEach(
              ((accountType, combinedPnL) -> {
                Double realizedImntWithDivPnL =
                    realizedWithDividendDatePnLMap.floorEntry(date).getValue().get(accountType);
                Double unrealizedPnL =
                    unrealizedDatePnLMap.containsKey(date)
                        ? unrealizedDatePnLMap.get(date).get(accountType)
                        : null;
                double cumulativeCombinedPnL =
                    sanitizeDouble(realizedImntWithDivPnL) + sanitizeDouble(unrealizedPnL);
                Map<MarketDataProto.AccountType, Double> typeDoubleMap =
                    combinedDatePnLCumulativeMap.computeIfAbsent(date, k -> new HashMap<>());
                typeDoubleMap.put(accountType, cumulativeCombinedPnL);
              }));
        });
  }

  private int getNextMarketDate(int date) {
    LocalDate localDate = DateFormatUtil.getLocalDate(date);
    for (int i = 0; i <= 5; i++) {
      localDate = localDate.plusDays(1);
      if (dateAndLocalDateMap.containsKey(date)) return DateFormatUtil.getDate(localDate);
    }
    return -1; // what if div date is on a non trading date beyond last trading date of benchmark
    // ticker?
  }

  private int getDate(DataNode node) {
    return node.getInstrument().getTicker().getData(0).getDate();
  }

  public Set<String> getInstruments() {
    return marketData.keySet();
  }

  private List<MarketDataProto.AccountType> getAccountTypes() {
    return Arrays.stream(MarketDataProto.AccountType.values())
        .filter(t -> t != MarketDataProto.AccountType.UNRECOGNIZED)
        .toList();
  }

  record DividendRecord(String symbol, int date, double dividend, String orderId) {}
}
