package com.vv.personal.twm.portfolio.remote.market.outdated;

import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.portfolio.model.market.OutdatedSymbol;
import java.util.Optional;
import java.util.TreeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2025-12-31
 */
class OutdatedSymbolsTest {

  private OutdatedSymbols outdatedSymbols;

  @BeforeEach
  void setUp() {
    outdatedSymbols = new OutdatedSymbols();
    assertTrue(outdatedSymbols.loadFromFile("src/test/resources/OutdatedSymbols.txt"));
  }

  @Test
  void isCurrentDateOutdated() {
    assertTrue(outdatedSymbols.isCurrentDateOutdated("CASH.TO", 20200624));
    assertTrue(outdatedSymbols.isCurrentDateOutdated("^VIX", 20250311));
    assertTrue(outdatedSymbols.isCurrentDateOutdated("^VIX", 20250310));
    assertTrue(outdatedSymbols.isCurrentDateOutdated("STLC.TO", 20251231));

    assertFalse(outdatedSymbols.isCurrentDateOutdated("^VIX", 20251231));
    assertFalse(outdatedSymbols.isCurrentDateOutdated("CM.TO", 20251231));
  }

  @Test
  void contains() {
    assertTrue(outdatedSymbols.contains("^VIX"));
    assertTrue(outdatedSymbols.contains("STLC.TO"));
    assertTrue(outdatedSymbols.contains("CASH.TO"));
    assertTrue(outdatedSymbols.contains("DBM.TO"));

    assertFalse(outdatedSymbols.contains("CM.TO"));
    assertFalse(outdatedSymbols.contains("BNS.TO"));
  }

  @Test
  void get() {
    Optional<TreeSet<OutdatedSymbol>> outdated = outdatedSymbols.get("^VIX");
    assertTrue(outdated.isPresent());
    assertEquals(3, outdated.get().size());
    assertEquals(20240101, outdated.get().first().outdateStartDate());

    outdated = outdatedSymbols.get("STLC.TO");
    assertTrue(outdated.isPresent());
    assertEquals(1, outdated.get().size());

    assertFalse(outdatedSymbols.get("CM.TO").isPresent());
    assertFalse(outdatedSymbols.get("BNS.TO").isPresent());
  }

  @Test
  void isDelisted() {
    assertTrue(outdatedSymbols.isDelisted("STLC.TO"));
    assertFalse(outdatedSymbols.isDelisted("CM.TO"));
    assertFalse(outdatedSymbols.isDelisted("^VIX"));
  }
}
