package com.vv.personal.twm.portfolio.model;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Vivek
 * @since 2024-08-06
 */
public class AdjustedCostBase {

    private final ConcurrentHashMap<String, Pair<Map<MarketDataProto.AccountType, Node>, List<MarketDataProto.Investment>>> acbMap;

    public AdjustedCostBase() {
        acbMap = new ConcurrentHashMap<>();
    }

    public void addBlock(MarketDataProto.Investment investment) {
        Pair<Map<MarketDataProto.AccountType, Node>, List<MarketDataProto.Investment>> nodeListPair =
                acbMap.computeIfAbsent(investment.getTicker().getSymbol(),
                        k -> Pair.of(new HashMap<>(), new ArrayList<>()));

        Map<MarketDataProto.AccountType, Node> nodeMap = nodeListPair.getLeft();
        List<MarketDataProto.Investment> investmentList = nodeListPair.getRight();
        investmentList.add(investment);

        Node node = nodeMap.computeIfAbsent(investment.getAccountType(), k -> new Node());
        node.updateData(investment.getQty(), investment.getTicker().getData(0).getPrice()); // todo - verify if only the first instance is required
    }

    public double getAdjustedCost(String instrument, MarketDataProto.AccountType accountType) {
        double adjustedCost = 0.0;
        if (!acbMap.containsKey(instrument)
                || !acbMap.get(instrument).getKey().containsKey(accountType)
                || acbMap.get(instrument).getKey().get(accountType).getQuantity() <= 0)
            return adjustedCost;
        return acbMap.get(instrument).getKey().get(accountType).getTotalCost()
                / acbMap.get(instrument).getKey().get(accountType).getQuantity();
    }

    public List<MarketDataProto.Investment> getInvestmentData(String instrument) {
        return acbMap.getOrDefault(instrument, Pair.of(new HashMap<>(), new ArrayList<>())).getRight();
    }

    public List<MarketDataProto.Investment> getInvestmentData(String instrument, MarketDataProto.AccountType accountType) {
        return acbMap.getOrDefault(instrument, Pair.of(new HashMap<>(), new ArrayList<>())).getRight()
                .stream().filter(investment -> investment.getAccountType() == accountType)
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
