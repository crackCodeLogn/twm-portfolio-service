package com.vv.personal.twm.portfolio.service.impl;

import static com.vv.personal.twm.portfolio.model.tracking.ProgressTracker.POSITION_ZERO;

import com.vv.personal.twm.portfolio.model.tracking.ProgressTracker;
import com.vv.personal.twm.portfolio.service.ProgressTrackerService;
import io.micrometer.common.util.StringUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2025-11-30
 */
@Slf4j
@Service
public class ProgressTrackerServiceImpl implements ProgressTrackerService {
  private final Map<String, ProgressTracker> progressTrackerMap;

  public ProgressTrackerServiceImpl() {
    progressTrackerMap = new ConcurrentHashMap<>();
  }

  @Override
  public void publishProgressTracker(String clientId, ProgressTracker progressTracker) {
    if (StringUtils.isEmpty(clientId)) {
      log.error("clientId is empty for publishProgressTracker");
      return;
    }

    if (progressTracker == null) {
      log.warn("Overriding progress tracker to {} due to null", POSITION_ZERO);
      progressTracker = POSITION_ZERO;
    }
    progressTrackerMap.put(clientId, progressTracker);
  }

  @Override
  public ProgressTracker getProgressTracker(String clientId) {
    if (StringUtils.isEmpty(clientId)) return POSITION_ZERO;
    return progressTrackerMap.getOrDefault(clientId, POSITION_ZERO);
  }

  Map<String, ProgressTracker> getProgressTrackerMap() {
    return progressTrackerMap;
  }
}
