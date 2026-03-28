package com.vv.personal.twm.portfolio.service;

import java.util.concurrent.ExecutorService;

/**
 * @author Vivek
 * @since 2026-03-27
 */
public interface ExecutorProviderService {

  ExecutorService procure(String executorName);

  ExecutorService procure(String executorName, int maxThreads);

  void shutdown(String executorName);

  void shutdownAllExecutors();
}
