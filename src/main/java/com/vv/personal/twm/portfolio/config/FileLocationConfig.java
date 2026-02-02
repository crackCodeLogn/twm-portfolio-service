package com.vv.personal.twm.portfolio.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author Vivek
 * @since 2024-12-07
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "locations")
public class FileLocationConfig {
  private String outdatedSymbols;
  private String imntMaxWeights;
  private String marketTransactionsBuy;
  private String marketTransactionsSell;
  private String marketTransactionsDivTfsa;
  private String marketTransactionsDivNr;
  private String marketTransactionsDivFhsa;
}
