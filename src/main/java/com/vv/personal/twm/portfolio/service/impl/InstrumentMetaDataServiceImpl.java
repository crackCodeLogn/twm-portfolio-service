package com.vv.personal.twm.portfolio.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import com.vv.personal.twm.portfolio.cache.InstrumentMetaDataCache;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataCrdbServiceFeign;
import com.vv.personal.twm.portfolio.remote.feign.MarketDataPythonEngineFeign;
import com.vv.personal.twm.portfolio.remote.market.outdated.OutdatedSymbols;
import com.vv.personal.twm.portfolio.service.InstrumentMetaDataService;
import com.vv.personal.twm.portfolio.util.DateFormatUtil;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.stereotype.Service;

/**
 * @author Vivek
 * @since 2025-12-22
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InstrumentMetaDataServiceImpl implements InstrumentMetaDataService {

  //  private static final List<String> dataFieldsForMetaDataUpdateViaMarketEngine =
  //      Lists.newArrayList("divYield");
  private static final String SECTOR_ETF_BONDS = "etf-bon";
  private static final String SECTOR_ETF_CASH = "etf-cas";
  private static final Set<String> OVERRIDE_INSTRUMENTS_DIV_YIELD_SKIP_UPDATE = Sets.newHashSet("");
  private static final Map<String, String> OVERRIDE_SECTOR_MAPPING =
      ImmutableMap.<String, String>builder()
          .put("Financial Services", "fin")
          .put("Energy", "energy")
          .put("Communication Services", "comms")
          .put("Utilities", "util")
          .put("Consumer Cyclical", "cons-cy")
          .put("Consumer Defensive", "cons-de")
          .put("Industrials", "industr")
          .put("Basic Materials", "basic-m")
          .put("Real Estate", "realest")
          .put("Technology", "tech")
          .put("Unknown", "etf-mkt")
          .build();
  private static final Map<String, String> OVERRIDE_IMNT_SECTOR_MAPPING =
      ImmutableMap.<String, String>builder().put("CASH.TO", SECTOR_ETF_CASH).build();
  private static final List<String> BETA_FIELDS = Lists.newArrayList("beta", "beta3Year");
  private static final String QUOTE_TYPE_FIELD = "quoteType";
  private static final String FORWARD_PE_FIELD = "forwardPE";

  private static final String KEY_MER = "mer";
  private static final String KEY_DIV_YIELD = "div-yield";
  private static final String KEY_SIGNAL = "signal";
  private static final String KEY_NOTES = "notes";
  private static final String KEY_ISSUE_COUNTRY = "issue-country";
  private static final String KEY_ORIGIN_COUNTRY = "origin-country";
  private static final String KEY_CCY = "ccy";
  private static final String KEY_SECTOR = "sector";
  private static final String KEY_IMNT_TYPE = "imnt-type";
  private static final String KEY_NAME = "name";

  private static final String JSON_KEY_CORP_ACTIONS = "corporateActions";
  private static final String JSON_KEY_COMP_OFFICERS = "companyOfficers";

  private final InstrumentMetaDataCache instrumentMetaDataCache;
  private final MarketDataCrdbServiceFeign marketDataCrdbServiceFeign;
  private final MarketDataPythonEngineFeign marketDataPythonEngineFeign;

  private final ObjectMapper mapper = new ObjectMapper();

  @Setter private int benchMarkCurrentDate = -1;
  @Setter private OutdatedSymbols outdatedSymbols;

  @Override
  public boolean load(int benchMarkCurrentDate, boolean forceReloadDataForCurrentDate) {
    setBenchMarkCurrentDate(benchMarkCurrentDate);
    log.info("Initiating complete instrument metadata load");
    try {
      log.info("Initiating read from db of entire metadata");
      MarketDataProto.Portfolio entireMetaData =
          marketDataCrdbServiceFeign.getEntireMarketMetaData();

      if (Objects.isNull(entireMetaData)) {
        log.warn("No meta data found in db!");
        return true;
      }

      log.info("Received {} instrument meta data from db", entireMetaData.getInstrumentsCount());
      boolean writeBackToDb = false;

      // checking for stale state and enforcing a reload if required
      for (MarketDataProto.Instrument instrument : entireMetaData.getInstrumentsList()) {
        String imnt = instrument.getTicker().getSymbol();
        if (outdatedSymbols != null && outdatedSymbols.isDelisted(imnt)) {
          log.info("Skipping load of delisted imnt: {}", imnt);
          continue;
        }

        MarketDataProto.Instrument.Builder imntBuilder =
            MarketDataProto.Instrument.newBuilder().mergeFrom(instrument);

        if (instrument.getTicker().getDataCount() > 0) {
          int metaDataDate = instrument.getTicker().getData(0).getDate();

          if (metaDataDate < benchMarkCurrentDate || forceReloadDataForCurrentDate) {
            log.info("\tComputing meta data for {}", imnt);
            if (!OVERRIDE_INSTRUMENTS_DIV_YIELD_SKIP_UPDATE.contains(imnt)) {
              // div-yield is the only thing getting updated for now from yfinance (mkt engine)
              Optional<Double> divYield = queryDividendYield(imnt);
              if (divYield.isPresent()) {
                imntBuilder.setDividendYield(divYield.get());
                writeBackToDb = true; // found a new update, thus need to write back to the db
              }
              imntBuilder.getTickerBuilder().clearData();
              imntBuilder
                  .getTickerBuilder()
                  .addData(
                      MarketDataProto.Value.newBuilder().setDate(benchMarkCurrentDate).build());
            }

            queryInfo(imnt, imntBuilder); // update the imnt with latest metadata
          }
          MarketDataProto.Instrument updatedImnt = imntBuilder.build();
          instrumentMetaDataCache.offer(updatedImnt);
        }
      }

      if (writeBackToDb) writeBackToDb();

      return true;
    } catch (Exception e) {
      log.warn("Unable to load instrument meta data from db", e);
    }
    return false;
  }

  @Override
  public void writeBackToDb() {
    MarketDataProto.Portfolio.Builder entireMetaData = MarketDataProto.Portfolio.newBuilder();

    log.info("Retrieving instrument meta data from cache");
    for (String imnt : instrumentMetaDataCache.getAllInstruments()) {
      Optional<MarketDataProto.Instrument> instrumentMetaData = instrumentMetaDataCache.get(imnt);
      instrumentMetaData.ifPresent(entireMetaData::addInstruments);
    }
    MarketDataProto.Portfolio entireMetaDataPortfolio = entireMetaData.build();
    log.info("Initiating write to db of entire metadata");
    try {
      String result =
          marketDataCrdbServiceFeign.addMarketMetaDataForPortfolio(true, entireMetaDataPortfolio);
      log.info("Result of db writeback: {}", result);
    } catch (Exception e) {
      log.error("Unable to write instrument meta data to db", e);
    }
  }

  @Override
  public void clear() {
    log.warn("Initiating instrument metadata clearing");
    instrumentMetaDataCache.flush();
    log.info("Completed instrument metadata clearing");
  }

  @Override
  public Map<String, Double> getAllImntsDividendYieldPercentage() {
    Map<String, Double> dividendYieldPercentage = new HashMap<>();
    for (String imnt : instrumentMetaDataCache.getAllInstruments()) {
      Optional<MarketDataProto.Instrument> instrumentMetaData = instrumentMetaDataCache.get(imnt);
      instrumentMetaData.ifPresent(
          instrument -> dividendYieldPercentage.put(imnt, instrument.getDividendYield()));
    }
    return dividendYieldPercentage;
  }

  @Override
  public Optional<Double> getDividendYield(String imnt) {
    return instrumentMetaDataCache.get(imnt).map(MarketDataProto.Instrument::getDividendYield);
  }

  @Override
  public Optional<Double> getManagementExpenseRatio(String imnt) {
    return instrumentMetaDataCache.get(imnt).map(MarketDataProto.Instrument::getMer);
  }

  @Override
  public Optional<Double> getBeta(String imnt) {
    Optional<MarketDataProto.Instrument> instrumentMetaData = instrumentMetaDataCache.get(imnt);
    if (instrumentMetaData.isPresent()) {
      try {
        return getBeta(imnt, instrumentMetaData.get().getMetaDataMap());
      } catch (Exception e) {
        log.error("Failed to get beta for imnt {}", imnt, e);
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<Double> getPE(String imnt) {
    Optional<Double> pe = Optional.empty();
    Optional<MarketDataProto.Instrument> instrumentMetaData = instrumentMetaDataCache.get(imnt);
    if (instrumentMetaData.isPresent()) {
      String forwardPE =
          instrumentMetaData.get().getMetaDataOrDefault(FORWARD_PE_FIELD, StringUtils.EMPTY);
      if (forwardPE.isEmpty()) log.warn("Did not find PE for imnt {}", imnt);
      else pe = Optional.of(Double.parseDouble(forwardPE));
    }
    return pe;
  }

  @Override
  public MarketDataProto.InstrumentType getInstrumentType(String imnt) {
    Optional<MarketDataProto.Instrument> instrumentMetaData = instrumentMetaDataCache.get(imnt);
    return instrumentMetaData
        .map(v -> v.getTicker().getType())
        .orElse(MarketDataProto.InstrumentType.UNRECOGNIZED);
  }

  @Override
  public InstrumentMetaDataCache getInstrumentMetaDataCache() {
    return instrumentMetaDataCache;
  }

  @Override
  public MarketDataProto.Portfolio getEntireMetaData() {
    MarketDataProto.Portfolio.Builder portfolio = MarketDataProto.Portfolio.newBuilder();
    for (String imnt : instrumentMetaDataCache.getAllInstruments()) {
      Optional<MarketDataProto.Instrument> instrumentMetaData = instrumentMetaDataCache.get(imnt);
      instrumentMetaData.ifPresent(portfolio::addInstruments);
    }
    return portfolio.build();
  }

  @Override
  public MarketDataProto.Instrument getInstrumentMetaData(String imnt) {
    return instrumentMetaDataCache
        .get(imnt)
        .orElse(MarketDataProto.Instrument.newBuilder().build());
  }

  @Override
  public String upsertInstrumentMetaData(String imnt, DataPacketProto.DataPacket dataPacket) {
    Optional<MarketDataProto.Instrument> instrument = parseInstrument(imnt, dataPacket);

    if (instrument.isPresent()) {
      try {
        instrumentMetaDataCache.offer(instrument.get()); // cache write
        return marketDataCrdbServiceFeign.upsertMarketMetaDataForSingleTicker(
            imnt, instrument.get()); // db write
      } catch (Exception e) {
        log.error("Failed to upsert instrument meta data from cache", e);
      }
    }
    return "Failed";
  }

  @Override
  public String deleteInstrumentMetaData(String imnt) {
    try {
      instrumentMetaDataCache.remove(imnt);
      return marketDataCrdbServiceFeign.deleteMarketMetaDataByTicker(imnt);
    } catch (Exception e) {
      log.error("Unable to delete instrument meta data from db", e);
    }
    return "Failed";
  }

  @Override
  public String deleteEntireMetaData() {
    try {
      clear();
      return marketDataCrdbServiceFeign.truncateMetaData();
    } catch (Exception e) {
      log.error("Unable to delete instrument meta data from db", e);
    }
    return "Failed";
  }

  /**
   * Assumes a list of strings will be sent in, each entry having the following indexes
   *
   * <ol>
   *   <li>0: str: Currency Code (KEY_CCY)
   *   <li>1: double: Management Expense Ratio (KEY_MER)
   *   <li>2: str: Notes (KEY_NOTES)
   *   <li>3: str: Signal (KEY_SIGNAL)
   *   <li>4: str: Issue Country (KEY_ISSUE_COUNTRY)
   *   <li>5: str: Origin Country (KEY_ORIGIN_COUNTRY)
   *   <li>6: str: Sector (KEY_SECTOR) [can be empty to be queried on-demand]
   *   <li>7: double: Dividend Yield (KEY_DIV_YIELD) [can be empty to be queried on-demand]
   *   <li>8: str: Instrument Type (KEY_IMNT_TYPE)
   *   <li>9: str: Instrument Name (KEY_NAME)
   * </ol>
   *
   * @param dataPacket
   * @return
   */
  @Override
  public String bulkAddEntireMetaData(DataPacketProto.DataPacket dataPacket) {
    int passed = 0;
    try {
      for (String inputLine : dataPacket.getStringsList()) {
        String[] parts = StringUtils.split(inputLine.strip(), '|');
        if (parts.length >= 1) {
          String imnt = parts[0].strip();
          DataPacketProto.DataPacket.Builder packet = DataPacketProto.DataPacket.newBuilder();

          for (int i = 1; i < parts.length; i++) {
            String part = parts[i].strip();
            int index = part.indexOf('=');
            String key = part.substring(0, index).strip();
            String value = part.substring(index + 1).strip();
            packet.putStringStringMap(key, value);
          }

          String metadataUpsertResult = upsertInstrumentMetaData(imnt, packet.build());
          if (!metadataUpsertResult.equals("Failed")) passed++;
        } else {
          log.warn("Ignoring line for metadata parsing: {}", inputLine);
        }
      }
    } catch (Exception e) {
      log.error("Failed to truncateAndBulkAddEntireMetaData", e);
    }
    return String.valueOf(passed);
  }

  @Override
  public String reloadMetaDataCache() {
    try {
      clear();
      load(benchMarkCurrentDate, false);
      return "Done";
    } catch (Exception e) {
      log.error("Unable to clear instrument meta data from db", e);
    }
    return "Failed";
  }

  @Override
  public MarketDataProto.Portfolio getCorporateActionNews() {
    MarketDataProto.Portfolio.Builder portfolio = MarketDataProto.Portfolio.newBuilder();
    List<MarketDataProto.Instrument> imnts = new ArrayList<>();

    for (String imnt : instrumentMetaDataCache.getAllInstruments()) {
      Optional<MarketDataProto.Instrument> instrumentMetaData = instrumentMetaDataCache.get(imnt);
      if (instrumentMetaData.isPresent()
          && instrumentMetaData.get().getCorporateActionsCount() > 0) {
        MarketDataProto.Instrument.Builder imntBuilder = MarketDataProto.Instrument.newBuilder();
        imntBuilder.setTicker(
            MarketDataProto.Ticker.newBuilder()
                .setSymbol(instrumentMetaData.get().getTicker().getSymbol())
                .build());
        imntBuilder.addAllCorporateActions(instrumentMetaData.get().getCorporateActionsList());
        imnts.add(imntBuilder.build());
      }
    }
    imnts.sort(Comparator.comparing(v -> v.getTicker().getSymbol()));
    portfolio.addAllInstruments(imnts);
    return portfolio.build();
  }

  Optional<MarketDataProto.Instrument> parseInstrument(
      String imnt, DataPacketProto.DataPacket dataPacket) {
    try {
      MarketDataProto.Instrument.Builder imntBuilder = MarketDataProto.Instrument.newBuilder();
      MarketDataProto.Ticker.Builder tickerBuilder = MarketDataProto.Ticker.newBuilder();
      tickerBuilder.setSymbol(imnt);
      tickerBuilder.addData(
          MarketDataProto.Value.newBuilder()
              .setDate(DateFormatUtil.getDate(LocalDate.now()))
              .build());

      Map<String, String> kvMap =
          dataPacket.getStringStringMap(); // todo fix this in twm-artifactory?

      if (kvMap.containsKey(KEY_CCY)) {
        imntBuilder.setCcy(MarketDataProto.CurrencyCode.valueOf(kvMap.get(KEY_CCY)));
      }
      if (kvMap.containsKey(KEY_MER)) {
        imntBuilder.setMer(Double.parseDouble(kvMap.get(KEY_MER)));
      }
      if (kvMap.containsKey(KEY_NOTES)) {
        imntBuilder.setNotes(kvMap.get(KEY_NOTES));
      }
      if (kvMap.containsKey(KEY_SIGNAL)) {
        imntBuilder.setSignal(MarketDataProto.Signal.valueOf(kvMap.get(KEY_SIGNAL)));
      }
      if (kvMap.containsKey(KEY_ISSUE_COUNTRY)) {
        imntBuilder.setIssueCountry(MarketDataProto.Country.valueOf(kvMap.get(KEY_ISSUE_COUNTRY)));
      }
      if (kvMap.containsKey(KEY_ORIGIN_COUNTRY)) {
        imntBuilder.setOriginCountry(
            MarketDataProto.Country.valueOf(kvMap.get(KEY_ORIGIN_COUNTRY)));
      }
      if (kvMap.containsKey(KEY_SECTOR)) {
        tickerBuilder.setSector(kvMap.get(KEY_SECTOR));
      } else {
        querySector(imnt).ifPresent(tickerBuilder::setSector);
      }
      if (kvMap.containsKey(KEY_DIV_YIELD)) {
        imntBuilder.setDividendYield(Double.parseDouble(kvMap.get(KEY_DIV_YIELD)));
      } else {
        queryDividendYield(imnt).ifPresent(imntBuilder::setDividendYield);
      }
      if (kvMap.containsKey(KEY_NAME)) {
        tickerBuilder.setName(kvMap.get(KEY_NAME));
      } else {
        queryName(imnt).ifPresent(tickerBuilder::setName);
      }
      imntBuilder.setTicker(tickerBuilder);
      queryInfo(imnt, imntBuilder);

      Optional<MarketDataProto.Instrument> build = Optional.of(imntBuilder.build());
      log.debug(build.toString());
      return build;
    } catch (Exception e) {
      log.error("Failed to parse instrument correctly for {} from {}", imnt, dataPacket, e);
    }
    return Optional.empty();
  }

  Optional<String> queryName(String imnt) {
    log.info("Querying name for imnt {}", imnt);
    try {
      String name = marketDataPythonEngineFeign.getTickerName(imnt);
      return Optional.ofNullable(name);
    } catch (Exception e) {
      log.error("Failed to query name for imnt {}", imnt, e);
    }
    return Optional.empty();
  }

  Optional<String> querySector(String imnt) {
    log.info("Querying sector for imnt {}", imnt);
    try {
      String sector = marketDataPythonEngineFeign.getTickerSector(imnt);
      if (StringUtils.isEmpty(sector)) return Optional.empty();
      return Optional.of(OVERRIDE_SECTOR_MAPPING.getOrDefault(sector, sector));
    } catch (Exception e) {
      log.error("Failed to query sector for imnt {}", imnt, e);
    }
    return Optional.empty();
  }

  Optional<Double> queryDividendYield(String imnt) {
    log.info("Querying dividend yield for imnt {}", imnt);
    try {
      String divYield = marketDataPythonEngineFeign.getTickerDividend(imnt);

      if (StringUtils.isEmpty(divYield)) {
        log.warn("No dividend yield data found for {}", imnt);
      } else if (NumberUtils.isParsable(divYield)) {
        return Optional.of(Double.parseDouble(divYield));
      } else {
        log.warn("Unable to parse div yield data for {} => {}", imnt, divYield);
      }
    } catch (Exception e) {
      log.error("Failed to query market dividend data", e);
    }
    return Optional.empty();
  }

  void queryInfo(String imnt, MarketDataProto.Instrument.Builder imntBuilder) {
    log.info("Querying detailed info for imnt {}", imnt);
    try {
      String info = marketDataPythonEngineFeign.getTickerInfo(imnt);
      if (StringUtils.isEmpty(info)) {
        log.warn("No info data found for {}", imnt);
      } else {
        JsonNode root = mapper.readTree(info);
        populateInformationFromMap(root, imntBuilder, imnt);

        updateBeta(imnt, imntBuilder);
        updateImntType(imnt, imntBuilder);
        updateSector(imnt, imntBuilder);
      }
    } catch (Exception e) {
      log.error("Failed to query info for imnt {}", imnt, e);
    }
  }

  void populateInformationFromMap(
      JsonNode root, MarketDataProto.Instrument.Builder imntBuilder, String imnt) {
    handleCorporateActions(root, imntBuilder, imnt);
    handleCompanyOfficers(root, imntBuilder);

    Map<String, Object> result = mapper.convertValue(root, new TypeReference<>() {});
    result.remove(JSON_KEY_CORP_ACTIONS);
    result.remove(JSON_KEY_COMP_OFFICERS);

    Map<String, String> metadata = new HashMap<>();
    result.forEach(
        (metaKey, metaValue) -> {
          if (Objects.nonNull(metaValue)) metadata.put(metaKey, String.valueOf(metaValue));
        });
    imntBuilder.putAllMetaData(metadata);
  }

  private Optional<Double> getBeta(String imnt, Map<String, String> metadataMap) {
    for (String betaField : BETA_FIELDS) {
      String beta = metadataMap.get(betaField);
      if (!StringUtils.isEmpty(beta)) {
        try {
          return Optional.of(Double.parseDouble(beta));
        } catch (NumberFormatException e) {
          log.warn("Failed to convert beta {} to double", beta, e);
        }
      }
    }
    log.warn("Cannot find beta for {}", imnt);
    return Optional.empty();
  }

  private void updateBeta(String imnt, MarketDataProto.Instrument.Builder imntBuilder) {
    Optional<Double> beta = getBeta(imnt, imntBuilder.getMetaDataMap());
    if (beta.isEmpty()) {
      log.warn("Failed to update beta for {}", imnt);
      return;
    }
    imntBuilder.setBeta(beta.get());
  }

  private void updateImntType(String imnt, MarketDataProto.Instrument.Builder imntBuilder) {
    String value = imntBuilder.getMetaDataMap().get(QUOTE_TYPE_FIELD);
    if (!StringUtils.isEmpty(value)) {
      try {
        imntBuilder.getTickerBuilder().setType(MarketDataProto.InstrumentType.valueOf(value));
      } catch (Exception e) {
        log.warn("Failed to update imnt type for {}", imnt, e);
      }
    } else {
      log.warn("Failed to find quoteType for {}", imnt);
    }
  }

  private void updateSector(String imnt, MarketDataProto.Instrument.Builder imntBuilder) {
    if (OVERRIDE_IMNT_SECTOR_MAPPING.containsKey(imnt)) {
      imntBuilder.getTickerBuilder().setSector(OVERRIDE_IMNT_SECTOR_MAPPING.get(imnt));
      log.info("Overriding sector of {} to {}", imnt, OVERRIDE_IMNT_SECTOR_MAPPING.get(imnt));
    }

    if (imntBuilder.getTickerBuilder().getType() == MarketDataProto.InstrumentType.ETF
        && imntBuilder.getTickerBuilder().getName().toLowerCase().contains(" bond ")) {
      imntBuilder.getTickerBuilder().setSector(SECTOR_ETF_BONDS);
      log.info("Overriding sector of {} to {}", imnt, SECTOR_ETF_BONDS);
    }
  }

  private void handleCorporateActions(
      JsonNode root, MarketDataProto.Instrument.Builder imntBuilder, String imnt) {
    List<MarketDataProto.CorporateAction> corporateActions = new ArrayList<>();

    JsonNode corpActions = root.get(JSON_KEY_CORP_ACTIONS);
    if (Objects.nonNull(corpActions))
      corpActions
          .iterator()
          .forEachRemaining(
              node -> {
                MarketDataProto.CorporateAction.Builder corporateAction =
                    MarketDataProto.CorporateAction.newBuilder();

                corporateAction.setHeader(readValue(node, "header"));
                if ("Dividend".equalsIgnoreCase(corporateAction.getHeader()))
                  handleDividendCorporateAction(corporateAction, node);
                else log.warn("Unknown corporate action : {}", corporateAction.getHeader());

                corporateActions.add(corporateAction.build());
              });
    if (!corporateActions.isEmpty())
      log.info("Found {} dividend corporate actions for {}", corporateActions.size(), imnt);
    imntBuilder.clearCorporateActions();
    imntBuilder.addAllCorporateActions(corporateActions);
  }

  private void handleDividendCorporateAction(
      MarketDataProto.CorporateAction.Builder corporateAction, JsonNode node) {
    corporateAction.setMessage(readValue(node, "message"));

    JsonNode meta = node.get("meta");
    if (Objects.nonNull(meta)) {
      corporateAction.setMetaAmount(readValue(meta, "amount"));
      corporateAction.setMetaEventType(readValue(meta, "eventType"));

      JsonNode dateEpoch = meta.get("dateEpochMs");
      if (Objects.nonNull(dateEpoch)) {
        Date date = new Date(dateEpoch.asLong());
        corporateAction.setMetaDate(date.toString());
      }
    }
  }

  private void handleCompanyOfficers(
      JsonNode root, MarketDataProto.Instrument.Builder imntBuilder) {
    List<MarketDataProto.CompanyOfficer> companyOfficers = new ArrayList<>();

    JsonNode compOfficers = root.get(JSON_KEY_COMP_OFFICERS);
    if (Objects.nonNull(compOfficers))
      compOfficers
          .iterator()
          .forEachRemaining(
              node -> {
                MarketDataProto.CompanyOfficer.Builder companyOfficer =
                    MarketDataProto.CompanyOfficer.newBuilder();
                companyOfficer.setName(readValue(node, "name"));
                companyOfficer.setTitle(readValue(node, "title"));
                companyOfficer.setTotalPay(readValue(node, "totalPay"));
                companyOfficer.setFiscalYear(readValue(node, "fiscalYear"));
                companyOfficer.setAge(readValue(node, "age"));

                companyOfficers.add(companyOfficer.build());
              });
    imntBuilder.addAllCompanyOfficers(companyOfficers);
  }

  private String readValue(JsonNode node, String key) {
    JsonNode value = node.get(key);
    return Objects.nonNull(value) ? value.asText() : StringUtils.EMPTY;
  }
}
