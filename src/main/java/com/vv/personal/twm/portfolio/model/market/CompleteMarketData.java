package com.vv.personal.twm.portfolio.model.market;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.service.TickerDataWarehouseService;
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

  // Holds map of ticker x (map of account type x doubly linked list nodes of transactions done)
  private final Map<String, Map<MarketDataProto.AccountType, DataList>> marketData;
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>> realizedPnLMap;
  private final Map<Integer, Map<MarketDataProto.AccountType, Double>> unrealizedPnLMap;
  private TickerDataWarehouseService tickerDataWarehouseService;

  public CompleteMarketData() {
    marketData = new ConcurrentHashMap<>();
    realizedPnLMap = Collections.synchronizedMap(new TreeMap<>());
    unrealizedPnLMap = Collections.synchronizedMap(new TreeMap<>());

    tickerDataWarehouseService = null;
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
    marketData.forEach(
        (imnt, typeDataMap) ->
            typeDataMap.forEach(
                (type, dataList) -> {
                  DataNode node = dataList.getHead();
                  while (node != null) {
                    int date = node.getInstrument().getTicker().getData(0).getDate();
                    Double marketPrice = tickerDataWarehouseService.getMarketData(imnt, date);
                    computeUnrealizedPnL(imnt, type, node, date, marketPrice);
                    if (node.getInstrument().getDirection() == MarketDataProto.Direction.BUY) {
                      node = node.getNext();
                      continue;
                    }
                    computeRealizedPnL(imnt, type, node, date, marketPrice);

                    node = node.getNext();
                  }
                }));
  }

  private void computeRealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getInstrument().getQty();

    double tickerPrice = node.getPrev().getAcb().getAcbPerShare();
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
    realizedPnLMap.computeIfAbsent(
        date,
        k -> {
          Map<MarketDataProto.AccountType, Double> typePriceMap = new HashMap<>();
          Arrays.stream(MarketDataProto.AccountType.values())
              .filter(t -> t != MarketDataProto.AccountType.UNRECOGNIZED)
              .forEach(accountType -> typePriceMap.put(accountType, 0.0));
          return typePriceMap;
        });

    Map<MarketDataProto.AccountType, Double> typePriceMap = realizedPnLMap.get(date);
    typePriceMap.put(type, typePriceMap.get(type) + pnL);
  }

  private void computeUnrealizedPnL(
      String imnt, MarketDataProto.AccountType type, DataNode node, int date, Double marketPrice) {
    double sellQty = node.getRunningQuantity();

    double tickerPrice = node.getAcb().getAcbPerShare();
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
    unrealizedPnLMap.computeIfAbsent(
        date,
        k -> {
          Map<MarketDataProto.AccountType, Double> typePriceMap = new HashMap<>();
          Arrays.stream(MarketDataProto.AccountType.values())
              .filter(t -> t != MarketDataProto.AccountType.UNRECOGNIZED)
              .forEach(accountType -> typePriceMap.put(accountType, 0.0));
          return typePriceMap;
        });

    Map<MarketDataProto.AccountType, Double> typePriceMap = unrealizedPnLMap.get(date);
    typePriceMap.put(type, typePriceMap.get(type) + pnL);
  }

  public Set<String> getInstruments() {
    return marketData.keySet();
  }
}
