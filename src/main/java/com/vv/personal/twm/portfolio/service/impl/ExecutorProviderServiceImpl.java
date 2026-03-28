package com.vv.personal.twm.portfolio.service.impl;

import com.vv.personal.twm.portfolio.service.ExecutorProviderService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2026-03-27
 */
@Service
@Slf4j
public class ExecutorProviderServiceImpl implements ExecutorProviderService {
  private final ConcurrentHashMap<String, ExecutorService> executorMap = new ConcurrentHashMap<>();

  @Override
  public ExecutorService procure(String executorName) {
    return procure(executorName, 1);
  }

  @Override
  public ExecutorService procure(String executorName, int maxThreads) {
    if (executorMap.containsKey(executorName)) return executorMap.get(executorName);
    return executorMap.computeIfAbsent(
        executorName, key -> Executors.newFixedThreadPool(maxThreads));
  }

  @Override
  public void shutdown(String executorName) {
    if (executorMap.containsKey(executorName)) {
      gracefulShutdown(executorName, executorMap.get(executorName));
      executorMap.remove(executorName);
    }
  }

  @Override
  public void shutdownAllExecutors() {
    log.info("Initiating shutdown of all executors");
    executorMap.forEach(this::gracefulShutdown);
    executorMap.clear();
    log.info("All executors shut down complete");
  }

  private void gracefulShutdown(String executorName, ExecutorService executorService) {
    try {
      log.info("Shutting down executor {}", executorName);
      if (!executorService.isShutdown()) executorService.shutdown();
      int wait = 10;
      while (!executorService.isShutdown() && wait-- > 0) Thread.sleep(1000);
    } catch (Exception e) {
      executorService.shutdownNow();
    } finally {
      if (!executorService.isShutdown()) executorService.shutdownNow();
    }
  }
}
