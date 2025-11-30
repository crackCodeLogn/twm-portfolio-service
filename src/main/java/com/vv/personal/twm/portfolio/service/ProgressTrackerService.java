package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.portfolio.model.tracking.ProgressTracker;

/**
 * @author Vivek
 * @since 2025-11-30
 */
public interface ProgressTrackerService {
  void publishProgressTracker(String clientId, ProgressTracker progressTracker);

  ProgressTracker getProgressTracker(String clientId);
}
