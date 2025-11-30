package com.vv.personal.twm.portfolio.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.vv.personal.twm.portfolio.model.tracking.ProgressTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Vivek
 * @since 2025-11-30
 */
class ProgressTrackerServiceImplTest {

  private ProgressTrackerServiceImpl progressTrackerService;

  @BeforeEach
  void setUp() {
    progressTrackerService = new ProgressTrackerServiceImpl();
  }

  // -------------------------------------------------------------------------------------
  // ## publishProgressTracker Tests
  // -------------------------------------------------------------------------------------

  @Test
  void publishProgressTracker_ShouldAddNewClientAndState() {
    String clientId = "client-A";
    ProgressTracker state = ProgressTracker.LOADING;

    progressTrackerService.publishProgressTracker(clientId, state);
    assertEquals(state, progressTrackerService.getProgressTracker(clientId));
  }

  @Test
  void publishProgressTracker_ShouldOverwriteExistingClientState() {
    String clientId = "client-B";
    progressTrackerService.publishProgressTracker(clientId, ProgressTracker.READY);
    ProgressTracker newState = ProgressTracker.LOADING_MARKET_COMPUTE_PNL;

    progressTrackerService.publishProgressTracker(clientId, newState);
    assertEquals(newState, progressTrackerService.getProgressTracker(clientId));
  }

  @Test
  void publishProgressTracker_ShouldSetPositionZeroWhenInputStateIsNull() {
    String clientId = "client-C";

    progressTrackerService.publishProgressTracker(clientId, null);
    assertEquals(
        ProgressTracker.POSITION_ZERO, progressTrackerService.getProgressTracker(clientId));
  }

  @Test
  void publishProgressTracker_ShouldNotPublishWhenClientIdIsEmpty() {
    // Test for empty string
    progressTrackerService.publishProgressTracker("", ProgressTracker.LOADING);
    // Test for null string
    progressTrackerService.publishProgressTracker(null, ProgressTracker.LOADING);

    assertTrue(progressTrackerService.getProgressTrackerMap().isEmpty());
  }

  // -------------------------------------------------------------------------------------
  // ## getProgressTracker Tests
  // -------------------------------------------------------------------------------------

  @Test
  void getProgressTracker_ShouldReturnDefaultStateForNonExistingClient() {
    ProgressTracker result = progressTrackerService.getProgressTracker("non-existent-client");
    assertEquals(ProgressTracker.POSITION_ZERO, result);
  }

  @Test
  void getProgressTracker_ShouldReturnDefaultStateForNullClient() {
    ProgressTracker result = progressTrackerService.getProgressTracker(null);
    assertEquals(ProgressTracker.POSITION_ZERO, result);
  }
}
