package com.vv.personal.twm.portfolio.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author Vivek
 * @since 2024-09-13
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "warehouse")
public class TickerDataWarehouseConfig {
  private boolean load;
  private int lookBackYears;
  private int benchmarkStartDate;
  private String benchmarkTicker;
}
