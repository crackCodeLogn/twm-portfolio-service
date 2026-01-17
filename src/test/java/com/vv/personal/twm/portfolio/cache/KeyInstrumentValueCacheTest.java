package com.vv.personal.twm.portfolio.cache;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2026-01-16
 */
class KeyInstrumentValueCacheTest {

  private KeyInstrumentValueCache cache;

  @BeforeEach
  void setUp() {
    cache = new KeyInstrumentValueCache();
  }

  @Test
  @DisplayName("Should store and retrieve a value correctly using Canadian tickers")
  void testOfferAndGet() {
    cache.offer("EQUITY_DESK", "CM.TO", 82.50);

    Optional<Double> result = cache.get("EQUITY_DESK", "CM.TO");

    assertTrue(result.isPresent());
    assertEquals(82.50, result.get());
  }

  @Test
  @DisplayName("Should return empty Optional for missing keys or instruments")
  void testGetMissingData() {
    cache.offer("FX_DESK", "BNS.TO", 71.20);

    assertAll(
        () -> assertTrue(cache.get("BOND_DESK", "BNS.TO").isEmpty()),
        () -> assertTrue(cache.get("FX_DESK", "TD.TO").isEmpty()));
  }

  @Test
  @DisplayName("Should return removed value and verify deletion")
  void testRemoveReturnsValue() {
    cache.offer("PORTFOLIO_A", "CM.TO", 85.00);

    // Testing the updated remove logic
    Double removedValue = cache.remove("PORTFOLIO_A", "CM.TO");

    assertAll(
        () -> assertEquals(85.00, removedValue, "Should return the value previously stored"),
        () -> assertFalse(cache.containsImntForKey("PORTFOLIO_A", "CM.TO")),
        () ->
            assertNull(
                cache.remove("PORTFOLIO_A", "BNS.TO"), "Should return null if instrument missing"));
  }

  @Test
  @DisplayName("Should return null when removing from a non-existent key")
  void testRemoveFromMissingKey() {
    assertNull(cache.remove("NON_EXISTENT", "CM.TO"));
  }

  @Test
  @DisplayName("Should verify existence of keys and instruments")
  void testContainsMethods() {
    cache.offer("BANK_SECTOR", "BNS.TO", 70.00);

    assertAll(
        () -> assertTrue(cache.containsKey("BANK_SECTOR")),
        () -> assertTrue(cache.containsImntForKey("BANK_SECTOR", "BNS.TO")),
        () -> assertFalse(cache.containsImntForKey("BANK_SECTOR", "RY.TO")));
  }

  @Test
  @DisplayName("Should flush specific key and all its instruments")
  void testFlushKey() {
    cache.offer("GROUP_1", "CM.TO", 80.0);
    cache.offer("GROUP_2", "BNS.TO", 70.0);

    cache.flushKey("GROUP_1");

    assertAll(
        () -> assertFalse(cache.containsKey("GROUP_1")),
        () -> assertTrue(cache.containsKey("GROUP_2")));
  }

  @Test
  @DisplayName("Should clear all data on flushAll")
  void testFlushAll() {
    cache.offer("K1", "CM.TO", 10.0);
    cache.offer("K2", "BNS.TO", 20.0);

    cache.flushAll();

    assertAll(
        () -> assertFalse(cache.containsKey("K1")),
        () -> assertFalse(cache.containsKey("K2")),
        () -> assertFalse(cache.containsImntForKey("K1", "CM.TO")));
  }
}
