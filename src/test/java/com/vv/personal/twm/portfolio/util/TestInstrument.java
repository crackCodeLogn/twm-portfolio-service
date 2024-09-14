package com.vv.personal.twm.portfolio.util;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import lombok.Builder;

/**
 * @author Vivek
 * @since 2024-09-13
 */
@Builder
public class TestInstrument {

  @Builder.Default private String symbol = "test.to";
  @Builder.Default private String name = "test";
  @Builder.Default private String sector = "test";

  @Builder.Default
  private MarketDataProto.InstrumentType instrumentType = MarketDataProto.InstrumentType.EQUITY;

  @Builder.Default
  private MarketDataProto.AccountType accountType = MarketDataProto.AccountType.TFSA;

  @Builder.Default private double qty = 0.0;
  @Builder.Default private double price = 0.0;
  @Builder.Default private MarketDataProto.Direction direction = MarketDataProto.Direction.BUY;
  @Builder.Default private int date = 0;

  public MarketDataProto.Instrument getInstrument() {
    return MarketDataProto.Instrument.newBuilder()
        .setQty(qty)
        .setDirection(direction)
        .setAccountType(accountType)
        .setTicker(
            MarketDataProto.Ticker.newBuilder()
                .setSymbol(symbol.toUpperCase())
                .setName(name.toUpperCase())
                .setSector(sector.toUpperCase())
                .setType(instrumentType)
                .addData(MarketDataProto.Value.newBuilder().setDate(date).setPrice(price).build())
                .build())
        .build();
  }
}
