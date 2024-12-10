package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.config.OutdatedSymbols;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

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
  // post processes, i.e. not filled during startup
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>> realizedDatePnLMap;
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedDatePnLMap;
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>> combinedDatePnLMap;
  private final Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>>
      unrealizedImntPnLMap;
  private final Map<String, Map<MarketDataProto.AccountType, Map<Integer, Double>>>
      realizedImntPnLMap;
  private final Map<LocalDate, Integer> localDateAndDateMap;
  private final Map<Integer, LocalDate> dateAndLocalDateMap;
  private TickerDataWarehouseService tickerDataWarehouseService;
  private OutdatedSymbols outdatedSymbols;

  public CompleteMarketData() {
    marketData = new ConcurrentHashMap<>();
    realizedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    unrealizedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    combinedDatePnLMap = Collections.synchronizedMap(new TreeMap<>());
    unrealizedImntPnLMap = new ConcurrentHashMap<>();
    realizedImntPnLMap = new ConcurrentHashMap<>();

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
          int intDate = DateFormatUtil.getLocalDate(date);
          localDateAndDateMap.put(date, intDate);
          dateAndLocalDateMap.put(intDate, date);
        });

    boolean failed = false;
    outer:
    for (Map.Entry<String, Map<MarketDataProto.AccountType, DataList>> marketDataEntry :
        marketData.entrySet()) {
      String imnt = marketDataEntry.getKey();
      Map<MarketDataProto.AccountType, DataList> typeDataMap = marketDataEntry.getValue();
      for (Map.Entry<MarketDataProto.AccountType, DataList> entry : typeDataMap.entrySet()) {
        MarketDataProto.AccountType type = entry.getKey();
        DataList dataList = entry.getValue();
        DataNode node = dataList.getHead();
        int nodeDate = getDate(node);
        int dateIndex = 0;

        while (dateIndex < dates.size()
            && localDateAndDateMap.get(dates.get(dateIndex)) != nodeDate) {
          dateIndex++;
        }

        log.info("Found dateIndex: {} for {} of {} {}", dateIndex, nodeDate, imnt, type);
        while (dateIndex < dates.size()) {
          int date = localDateAndDateMap.get(dates.get(dateIndex));
          if (node.getNext() != null && getDate(node.getNext()) == date) {
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
            } else {
              log.error("Did not find market price for {} x {}", imnt, date);
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
          computeRealizedPnL(imnt, type, node, date, marketPrice);

          dateIndex++;
        }
      }
    }
    unrealizedDatePnLMap.forEach(
        (k, v) -> System.out.printf("%d x %.6f\n", k, v.get(MarketDataProto.AccountType.NR)));

    if (!failed) computeCombinedPnL(dates);
    else log.error("Failed to compute pnL. Check logs for relevant error.");
  }

  private int getDate(DataNode node) {
    return node.getInstrument().getTicker().getData(0).getDate();
  }

  private void computeRealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getInstrument().getQty();

    double tickerPrice = node.getPrev().getAcb().getAcbPerUnit();
    double pnL = (marketPrice - tickerPrice) * sellQty;

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

    Map<MarketDataProto.AccountType, Double> typePriceMap = realizedDatePnLMap.get(date);
    typePriceMap.put(type, typePriceMap.get(type) + pnL);
    realizedImntPnLMap.get(imnt).get(type).put(date, pnL);
  }

  private void computeUnrealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getRunningQuantity();

    double tickerPrice = node.getAcb().getAcbPerUnit();
    double pnL = (marketPrice - tickerPrice) * sellQty;

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

    Map<MarketDataProto.AccountType, Double> typePriceMap = unrealizedDatePnLMap.get(date);
    typePriceMap.put(type, typePriceMap.get(type) + pnL);
    unrealizedImntPnLMap.get(imnt).get(type).put(date, pnL);
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
            Double combinedPnL = getValue(unrealizedPnL) + getValue(realizedPnL);

            log.info("combined pnL {} x {} => pnl= {}", date, type.name(), combinedPnL);

            Map<MarketDataProto.AccountType, Double> typePriceMap = combinedDatePnLMap.get(date);
            if (typePriceMap == null) typePriceMap = new HashMap<>();
            typePriceMap.put(type, combinedPnL);
            combinedDatePnLMap.put(date, typePriceMap);
          }
        });
  }

  public Set<String> getInstruments() {
    return marketData.keySet();
  }

  private List<MarketDataProto.AccountType> getAccountTypes() {
    return Arrays.stream(MarketDataProto.AccountType.values())
        .filter(t -> t != MarketDataProto.AccountType.UNRECOGNIZED)
        .toList();
  }

  private Double getValue(Double value) {
    return value == null ? 0.0 : value;
  }
}
