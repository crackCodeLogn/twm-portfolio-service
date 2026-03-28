package com.vv.personal.twm.portfolio.service.impl;

import com.vv.personal.twm.portfolio.service.DiscoveryClientService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2026-03-28
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DiscoveryClientServiceImpl implements DiscoveryClientService {

  private final DiscoveryClient discoveryClient;

  @Override
  public int getAppInstanceCount(String appName) {
    try {
      return discoveryClient.getInstances(appName).size();
    } catch (Exception e) {
      log.error("Failed to get instance count for appName={}", appName, e);
    }
    return -1;
  }
}
