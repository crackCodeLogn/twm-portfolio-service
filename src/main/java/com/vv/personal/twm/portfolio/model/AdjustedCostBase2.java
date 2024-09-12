package com.vv.personal.twm.portfolio.model;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author Vivek
 * @since 2024-08-06
 */
@Deprecated(forRemoval = true)
public class AdjustedCostBase2 {

  private final ConcurrentHashMap<
          String, Pair<Map<MarketDataProto.AccountType, Node>, List<MarketDataProto.Instrument>>>
      acbMap;

  public AdjustedCostBase2() {
    acbMap = new ConcurrentHashMap<>();
  }

  public void addBlock(MarketDataProto.Instrument instrument) {
    Pair<Map<MarketDataProto.AccountType, Node>, List<MarketDataProto.Instrument>> nodeListPair =
        acbMap.computeIfAbsent(
            instrument.getTicker().getSymbol(), k -> Pair.of(new HashMap<>(), new ArrayList<>()));

    Map<MarketDataProto.AccountType, Node> nodeMap = nodeListPair.getLeft();
    List<MarketDataProto.Instrument> instrumentList = nodeListPair.getRight();
    instrumentList.add(instrument);

    Node node = nodeMap.computeIfAbsent(instrument.getAccountType(), k -> new Node());
    node.updateData(
        instrument.getQty(),
        instrument
            .getTicker()
            .getData(0)
            .getPrice()); // todo - verify if only the first instance is required
  }

  public double getAdjustedCost(String instrument, MarketDataProto.AccountType accountType) {
    double adjustedCost = 0.0;
    if (!acbMap.containsKey(instrument)
        || !acbMap.get(instrument).getKey().containsKey(accountType)
        || acbMap.get(instrument).getKey().get(accountType).getQuantity() <= 0) return adjustedCost;
    return acbMap.get(instrument).getKey().get(accountType).getTotalCost()
        / acbMap.get(instrument).getKey().get(accountType).getQuantity();
  }

  public List<MarketDataProto.Instrument> getInstrumentData(String instrument) {
    return acbMap.getOrDefault(instrument, Pair.of(new HashMap<>(), new ArrayList<>())).getRight();
  }

  public List<MarketDataProto.Instrument> getInstrumentData(
      String instrument, MarketDataProto.AccountType accountType) {
    return acbMap
        .getOrDefault(instrument, Pair.of(new HashMap<>(), new ArrayList<>()))
        .getRight()
        .stream()
        .filter(imnt -> imnt.getAccountType() == accountType)
        .collect(Collectors.toList());
  }

  public Set<MarketDataProto.AccountType> getAccountTypes() {
    Set<MarketDataProto.AccountType> accountTypes = new HashSet<>();
    acbMap.values().forEach(pair -> accountTypes.addAll(pair.getLeft().keySet()));
    return accountTypes;
  }

  public Set<String> getInstruments() {
    return acbMap.keySet();
  }

  @Getter
  @Setter
  private static final class Node {
    private double quantity;
    private double totalCost;

    public Node() {
      this.quantity = 0;
      this.totalCost = 0;
    }

    public void updateData(double quantity, double price) {
      this.quantity += quantity;
      this.totalCost += price * quantity;
    }
  }
}
