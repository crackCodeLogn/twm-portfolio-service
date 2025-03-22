package com.vv.personal.twm.portfolio.service;

/**
 * @author Vivek
 * @since 2025-03-22
 */
public interface ReloadService {

  boolean initialFullLoad();

  // Reload for v2k only [me]
  boolean reload();

  boolean reload(String uid);
}
