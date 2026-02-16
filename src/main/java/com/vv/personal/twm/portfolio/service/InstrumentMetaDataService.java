package com.vv.personal.twm.portfolio.service;

import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.cache.InstrumentMetaDataCache;
import com.vv.personal.twm.portfolio.remote.market.outdated.OutdatedSymbols;
import java.util.Map;
import java.util.Optional;

/**
 * @author Vivek
 * @since 2025-12-22
 */
public interface InstrumentMetaDataService {

  boolean load(int benchMarkCurrentDate, boolean forceReloadDataForCurrentDate);

  void writeBackToDb();

  void clear();

  Map<String, Double> getAllImntsDividendYieldPercentage();

  Optional<Double> getDividendYield(String imnt);

  Optional<Double> getManagementExpenseRatio(String imnt);

  Optional<Double> getBeta(String imnt);

  Optional<Double> getPE(String imnt);

  MarketDataProto.InstrumentType getInstrumentType(String imnt);

  Optional<String> getSector(String imnt);

  InstrumentMetaDataCache getInstrumentMetaDataCache();

  MarketDataProto.Portfolio getEntireMetaData();

  MarketDataProto.Instrument getInstrumentMetaData(String imnt);

  String upsertInstrumentMetaData(String imnt, DataPacketProto.DataPacket dataPacket);

  String deleteInstrumentMetaData(String imnt);

  String deleteEntireMetaData();

  String bulkAddEntireMetaData(DataPacketProto.DataPacket dataPacket);

  String reloadMetaDataCache();

  MarketDataProto.Portfolio getCorporateActionNews();

  void setOutdatedSymbols(OutdatedSymbols outdatedSymbols);

  String backup();
}
