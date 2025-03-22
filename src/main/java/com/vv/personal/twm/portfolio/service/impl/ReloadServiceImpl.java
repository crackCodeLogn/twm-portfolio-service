package com.vv.personal.twm.portfolio.service.impl;

import com.vv.personal.twm.portfolio.service.CompleteBankDataService;
import com.vv.personal.twm.portfolio.service.CompleteMarketDataService;
import com.vv.personal.twm.portfolio.service.ReloadService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2025-03-22
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class ReloadServiceImpl implements ReloadService {
  private final CompleteMarketDataService completeMarketDataService;
  private final CompleteBankDataService completeBankDataService;

  @Override
  public boolean initialFullLoad() {
    return reload(true);
  }

  @Override
  public boolean reload() {
    return reload(false);
  }

  boolean reload(boolean firstTimeLoad) {
    if (!firstTimeLoad) {
      log.warn("Clearing bank data service and market data service!");
      completeBankDataService.clear();
      completeMarketDataService.clear();
    }

    log.info("Starting reload...");
    StopWatch loadTimer = StopWatch.createStarted();
    try {
      completeBankDataService.load();
      completeMarketDataService.load();

      loadTimer.stop();
      log.info("Reload completed in {} s", loadTimer.getTime(TimeUnit.SECONDS));
      return true;
    } catch (Exception e) {
      log.error("Reload failed", e);
    } finally {
      if (!loadTimer.isStopped()) loadTimer.stop();
      loadTimer = null;
    }
    return false;
  }

  @Override
  public boolean reload(String uid) {
    throw new NotImplementedException("future task, not implemented yet");
  }
}
