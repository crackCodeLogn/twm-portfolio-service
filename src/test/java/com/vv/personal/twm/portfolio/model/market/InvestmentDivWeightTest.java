package com.vv.personal.twm.portfolio.model.market;

import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2024-11-11
 */
class InvestmentDivWeightTest {

  @Test
  public void testInvestmentDivWeight() {
    InvestmentDivWeight divWeight = new InvestmentDivWeight(400.0, 300.0);
    divWeight.addHighValueAllocation(MarketDataProto.AccountType.TFSA, "VFV", 100.00, 1.0);
    divWeight.addHighDivAllocation(MarketDataProto.AccountType.TFSA, "RY", 75.0, .25);
    divWeight.addHighDivAllocation(MarketDataProto.AccountType.TFSA, "CM", 75.0, .25);
    divWeight.addHighDivAllocation(MarketDataProto.AccountType.TFSA, "BMO", 75.0, .25);
    divWeight.addHighDivAllocation(MarketDataProto.AccountType.TFSA, "TD", 75.0, .25);

    divWeight.addHighValueAllocation(MarketDataProto.AccountType.NR, "VFV", 100.00, 1.0);
    divWeight.addHighDivAllocation(MarketDataProto.AccountType.NR, "BNS", 100.0, .50);
    divWeight.addHighDivAllocation(MarketDataProto.AccountType.NR, "CM", 100.0, .50);

    System.out.println(divWeight);
    String divWeightStr = divWeight.toString();
    divWeightStr = divWeightStr.substring(divWeightStr.indexOf("\n") + 1);
    assertEquals(
        "-----------------------------\n"
            + "Total amount investing in TFSA: $400.00\n"
            + "\n"
            + "*********************\n"
            + "For TFSA-VAL, money to be allocated as: 100.00\n"
            + "1. VFV: $100.00 -> 100.00%, left: $0.00\n"
            + "\n"
            + "*********************\n"
            + "For TFSA-DIV, money to be allocated as: 300.00\n"
            + "1. RY: $75.00 -> 25.00%, left: $225.00\n"
            + "2. CM: $75.00 -> 25.00%, left: $150.00\n"
            + "3. BMO: $75.00 -> 25.00%, left: $75.00\n"
            + "4. TD: $75.00 -> 25.00%, left: $0.00\n"
            + "\n"
            + "-----------------------------\n"
            + "Total amount investing in Non-registered: $300.00\n"
            + "\n"
            + "*********************\n"
            + "For NR-VAL, money to be allocated as: 100.00\n"
            + "1. VFV: $100.00 -> 100.00%, left: $0.00\n"
            + "\n"
            + "*********************\n"
            + "For NR-DIV, money to be allocated as: 200.00\n"
            + "1. BNS: $100.00 -> 50.00%, left: $100.00\n"
            + "2. CM: $100.00 -> 50.00%, left: $0.00\n"
            + "\n"
            + "-----------------------------",
        divWeightStr);
  }
}
