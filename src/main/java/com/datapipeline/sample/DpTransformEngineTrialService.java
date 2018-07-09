package com.datapipeline.sample;

import static com.datapipeline.clients.DpConnectorConfigKey.DP_SCHEMA_LIST;

import com.datapipeline.clients.DpAES;
import com.datapipeline.clients.DpConnectorConfigKey;
import com.datapipeline.clients.codeengine.CodeEngines;
import com.datapipeline.clients.connector.schema.base.DpSinkRecord;
import com.datapipeline.clients.connector.schema.base.DpSinkTaskSchemaContext;
import com.datapipeline.clients.record.DpRecordConstants;
import com.datapipeline.clients.record.DpRecordMessageType;
import com.datapipeline.clients.utils.JsonConvert;
import com.datapipeline.clients.zk.DpZkUtils;
import com.datapipeline.source.connector.filesystem.FileSystemSourceConnector;
import com.datapipeline.source.connector.filesystem.FileSystemSourceTask;
import com.datapipeline.source.couchbase.CouchbaseSourceConnector;
import com.datapipeline.source.couchbase.CouchbaseSourceTask;
import com.datapipeline.source.kafka.KafkaSourceConnector;
import com.datapipeline.source.kafka.KafkaSourceTask;
import com.datapipeline.source.mysql.MysqlBatchSourceConnector;
import com.datapipeline.source.mysql.MysqlBatchSourceTask;
import com.datapipeline.source.oracle.OracleSourceConnector;
import com.datapipeline.source.oracle.OracleSourceTask;
import com.datapipeline.source.postgres.PostgresSourceConnector;
import com.datapipeline.source.postgres.PostgresSourceTask;
import com.datapipeline.source.sqlserver.SqlServerSourceConnector;
import com.datapipeline.source.sqlserver.SqlServerSourceTask;
import com.dp.internal.bean.CodeEngineTrialResponseBean;
import com.dp.internal.bean.DataPipelineActiveBean;
import com.dp.internal.bean.DpDataSourceType;
import com.dp.internal.bean.DpSchemaBean;
import com.dp.internal.bean.PipelineConfig.DataSourceChangeConfig;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorTask;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DpTransformEngineTrialService {

  private static final Logger logger = LoggerFactory.getLogger(DpTransformEngineTrialService.class);

  private static final int READ_NUMBER_FACTOR = 5;

  private static final Random RANDOM_GENERATOR = new Random();

  private final LoadingCache<String, List<SourceRecord>> sampleCache =
      CacheBuilder.newBuilder()
          .maximumSize(10)
          .build(
              new CacheLoader<String, List<SourceRecord>>() {
                @Override
                public List<SourceRecord> load(String key) throws Exception {
                  throw new IOException("Can not find cached records");
                }
              });

  public CodeEngineTrialResponseBean getSamplesAndTrial(
      String dataPipelineId, String dpSchemaName, int limit, String sessionId) throws Exception {
    CodeEngineTrialResponseBean response = new CodeEngineTrialResponseBean();
    if (StringUtils.isBlank(sessionId)) {
      // get samples
      sessionId = generateCacheKey(dataPipelineId, dpSchemaName);
      ArrayNode samples = extractData(dataPipelineId, dpSchemaName, limit, sessionId, false);
      response.setSamples(JsonConvert.getJsonString(samples));
      response.setSessionId(sessionId);
    } else {
      // trial
      ArrayNode samples = extractData(dataPipelineId, dpSchemaName, limit, sessionId, false);
      ArrayNode trialResult = extractData(dataPipelineId, dpSchemaName, limit, sessionId, true);
      response.setSamples(JsonConvert.getJsonString(samples));
      response.setResults(JsonConvert.getJsonString(trialResult));
      response.setSessionId(sessionId);
    }
    return response;
  }

  private ArrayNode extractData(
      String dataPipelineId,
      String dpSchemaName,
      int limit,
      String sessionId,
      boolean codeEnginesFlag)
      throws Exception {
    DataPipelineActiveBean activeBean = DpZkUtils.DP_ZK_UTILS.readTaskConfig(dataPipelineId);
    List<SourceRecord> records;
    try {
      records = sampleCache.get(sessionId);
    } catch (ExecutionException e) {
      records = pollSourceRecords(dataPipelineId, dpSchemaName, limit, activeBean);
      sampleCache.put(sessionId, records);
    }
    if (records.size() == 1) {
      if (((Struct) records.get(0).value()).get("after") == null) {
        ArrayNode samples = JsonConvert.getObjectMapper().createArrayNode();
        return samples;
      }
    }
    List<DpSchemaBean> dpSchemaBeans = activeBean.getSchemas();
    List<DpSchemaBean> filterSchemaBeans;
    if (activeBean.getDataSource().getDataSourceType() != DpDataSourceType.FILE_SYSTEM) {
      filterSchemaBeans =
          dpSchemaBeans
              .stream()
              .filter(dpSchemaBean -> dpSchemaBean.getSourceSchemaName().equals(dpSchemaName))
              .collect(Collectors.toList());
    } else {
      filterSchemaBeans = dpSchemaBeans;
    }
    if (filterSchemaBeans.isEmpty()) {
      throw new IllegalArgumentException(String.format("can not find schema %s", dpSchemaName));
    }
    if (codeEnginesFlag) {
      CodeEngines codeEngines = new CodeEngines(filterSchemaBeans.get(0));
      return transformRecordsToSinkRecords(
          records, codeEngines, activeBean, filterSchemaBeans.get(0));
    } else {
      return transformRecordsToSinkRecords(records, null, activeBean, filterSchemaBeans.get(0));
    }
  }

  private List<SourceRecord> pollSourceRecords(
      String dataPipelineId, String dpSchemaName, int limit, DataPipelineActiveBean activeBean)
      throws IOException, InterruptedException {
    List<SourceRecord> records = new ArrayList<>();
    DpDataSourceType dpDataSourceType = activeBean.getDataSource().getDataSourceType();
    Map<String, String> properties = new HashMap<>();
    SourceConnector sourceConnector = sourceConnectorMapper(dpDataSourceType);
    SourceTask sourceTask = sourceTaskMapper(dpDataSourceType);
    try {
      properties.put(DpConnectorConfigKey.DPTASK_ID, dataPipelineId);
      properties.put(DpConnectorConfigKey.TRIAL, "true");
      sourceConnector.start(properties);
      List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(1);
      if (!taskConfigs.isEmpty()) {
        Map<String, String> config = taskConfigs.get(0);
        List<DpSchemaBean> dpSchemaBeanList = activeBean.getSchemas();
        List<DpSchemaBean> trialSchemaBean =
            dpSchemaBeanList
                .stream()
                .filter(dpSchemaBean -> dpSchemaBean.getSourceSchemaName().equals(dpSchemaName))
                .collect(Collectors.toList());
        config.put(
            DP_SCHEMA_LIST, JsonConvert.getObjectMapper().writeValueAsString(trialSchemaBean));
        if (dpDataSourceType == DpDataSourceType.MYSQL) {
          List<String> tableSchemaList = new ArrayList<>();
          List<String> tableWhitelist = new ArrayList<>();
          for (DpSchemaBean schemaBean : trialSchemaBean) {
            tableSchemaList.add(
                String.format(
                    "%s:%s",
                    schemaBean.getSourceSchemaName(), schemaBean.getDestiantionSchemaName()));
            tableWhitelist.add(
                String.format(
                    "%s.%s",
                    activeBean.getDataSource().getConfig().get("sqlDatabase").textValue(),
                    schemaBean.getSourceSchemaName()));
          }
          config.put(MySqlConnectorConfig.TABLE_WHITELIST.name(), String.join(",", tableWhitelist));
          config.put(MySqlConnectorConfig.TABLE_SCHEMA.name(), String.join(",", tableSchemaList));
          config.put("snapshot.locking.mode", "none");
          config.put("database.history.skip.unparseable.ddl", "true");
          config.put("database.history.store.only.monitored.tables.ddl", "true");
          config.put("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
        }
        sourceTask.start(config);
        long startTime = System.currentTimeMillis();
        int readLimit = limit * READ_NUMBER_FACTOR;
        while (System.currentTimeMillis() - startTime < 1000 * 45 && records.size() < readLimit) {
          List<SourceRecord> tempsRecords = sourceTask.poll();
          List<SourceRecord> filterRecords =
              tempsRecords
                  .stream()
                  .filter(
                      sourceRecord -> {
                        if (sourceRecord.valueSchema() != null
                            && sourceRecord.valueSchema().field(DpRecordConstants.DATA_KEY_AFTER)
                            != null) {
                          Struct sourceInfo =
                              (Struct)
                                  ((Struct) sourceRecord.value())
                                      .get(DpRecordConstants.DATA_KEY_SOURCE);
                          DpRecordMessageType type = null;
                          try {
                            type =
                                DpRecordMessageType.parse(
                                    sourceInfo.getString(
                                        DpRecordConstants.SOURCE_ENTITY_MESSAGE_TYPE));
                          } catch (DataException e) {
                            // ignore
                          }
                          return type == DpRecordMessageType.INSERT
                              || type == DpRecordMessageType.INSERT_TEMP
                              || type == null;
                        }
                        return false;
                      })
                  .collect(Collectors.toList());
          if (!tempsRecords.isEmpty()) {
            SourceRecord lastRecord = tempsRecords.get(tempsRecords.size() - 1);
            Struct sourceInfo =
                (Struct) ((Struct) lastRecord.value()).get(DpRecordConstants.DATA_KEY_SOURCE);
            DpRecordMessageType type =
                DpRecordMessageType.parse(
                    sourceInfo.getString(DpRecordConstants.SOURCE_ENTITY_MESSAGE_TYPE));
            if (type == DpRecordMessageType.SNAPSHOT_DONE) {
              // table is empty
              if (filterRecords.isEmpty()) {
                return Collections.singletonList(lastRecord);
              }
              // table is not empty and completed the snapshot
              else {
                records.addAll(filterRecords);
                if (records.size() >= limit) {
                  break;
                } else {
                  return records;
                }
              }
            }
          }
          records.addAll(filterRecords);
          try {
            Thread.sleep(100L);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
          }
        }
        if (records.isEmpty()) {
          throw new IllegalArgumentException("can not get samples");
        }
        return pickRandomN(records, limit);
      }
      throw new IllegalArgumentException("get taskConfig failed");
    } finally {
      sourceTask.stop();
    }
  }

  private <T> List<T> pickRandomN(List<T> input, int limit) {
    if (limit > input.size()) {
      return input;
    }
    List<T> result = new ArrayList<>();
    for (int i = 0; i < input.size(); i++) {
      int randomNumber = RANDOM_GENERATOR.nextInt(input.size() - i);
      if (randomNumber < limit) {
        result.add(input.get(i));
        limit--;
      }
      if (limit == 0) {
        break;
      }
    }
    return result;
  }

  private String generateCacheKey(String dataPipelineId, String dpSchemaName) {
    return DpAES.encrypt(
        String.join("_", dataPipelineId, dpSchemaName, String.valueOf(System.currentTimeMillis())));
  }

  private ArrayNode transformRecordsToSinkRecords(
      List<SourceRecord> records,
      CodeEngines codeEngines,
      DataPipelineActiveBean activeBean,
      DpSchemaBean dpSchemaBean)
      throws Exception {
    DpDataSourceType dataSourceType = activeBean.getDataSource().getDataSourceType();
    String dpTaskId = activeBean.getDataPipelineId().toString();
    String sourceSchemaName = dpSchemaBean.getSourceSchemaName();
    DataSourceChangeConfig dataSourceChangeConfig =
        !Objects.isNull(activeBean.getPipelineConfig())
            && !Objects.isNull(activeBean.getPipelineConfig().getMoreConfig())
            ? activeBean.getPipelineConfig().getMoreConfig().getDataSourceChangeConfig()
            : null;

    DpSinkTaskSchemaContext dpSinkTaskSchemaContext =
        DpSinkTaskSchemaContext.build(
            dpTaskId, sourceSchemaName, dataSourceType, dataSourceChangeConfig);

    if (CollectionUtils.isNotEmpty(dpSchemaBean.getFieldRelations())) {
      dpSinkTaskSchemaContext.setFidldsIntoColumnMapping(dpSchemaBean.getFieldRelations());
    }
    ArrayNode samples = JsonConvert.getObjectMapper().createArrayNode();
    for (SourceRecord sourceRecord : records) {
      SinkRecord sinkRecord =
          new SinkRecord(
              sourceRecord.topic(),
              0,
              sourceRecord.keySchema(),
              sourceRecord.key(),
              sourceRecord.valueSchema(),
              sourceRecord.value(),
              1);
      DpSinkRecord dpSinkRecord =
          new DpSinkRecord.Builder(
              (json, schema) -> {
                if (json != null) {
                  return json.toString();
                } else {
                  return "";
                }
              })
              .setSourceType(dataSourceType)
              .setCodeEngines(codeEngines)
              .setDpSinkSchemaContext(dpSinkTaskSchemaContext)
              .build(sinkRecord);
      if (!dpSinkRecord.isValid()) {
        throw dpSinkRecord.getInvalidCause();
      }
      String jsonString = dpSinkRecord.getFormattedLine();
      if (jsonString != null && !jsonString.isEmpty()) {
        logger.info(dpSinkRecord.getFormattedLine());
        samples.add(JsonConvert.getObjectMapper().readTree(dpSinkRecord.getFormattedLine()));
      }
    }
    return samples;
  }

  private SourceConnector sourceConnectorMapper(DpDataSourceType dpDataSourceType) {
    SourceConnector sourceConnector;
    switch (dpDataSourceType) {
      case MYSQL:
        sourceConnector = new MySqlConnector();
        break;
      case MYSQL_BATCH:
        sourceConnector = new MysqlBatchSourceConnector();
        break;
      case ORACLE:
        sourceConnector = new OracleSourceConnector();
        break;
      case SQL_SERVER:
        sourceConnector = new SqlServerSourceConnector();
        break;
      case FILE_SYSTEM:
        sourceConnector = new FileSystemSourceConnector();
        break;
      case COUCHBASE:
        sourceConnector = new CouchbaseSourceConnector();
        break;
      case FTP:
        sourceConnector = new FileSystemSourceConnector();
        break;
      case POSTGRES_BATCH:
        sourceConnector = new PostgresSourceConnector();
        break;
      case POSTGRES:
        sourceConnector = new PostgresConnector();
        break;
      case KAFKA:
        sourceConnector = new KafkaSourceConnector();
        break;
      default:
        throw new IllegalArgumentException("Source Type not support yet.");
    }
    return sourceConnector;
  }

  private SourceTask sourceTaskMapper(DpDataSourceType dpDataSourceType) {
    SourceTask sourceTask;
    WorkerSourceTaskContext context =
        new WorkerSourceTaskContext(
            new OffsetStorageReader() {
              @Override
              public <T> Map<String, Object> offset(Map<String, T> partition) {
                return null;
              }

              @Override
              public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                  Collection<Map<String, T>> partitions) {
                return null;
              }
            });
    switch (dpDataSourceType) {
      case MYSQL:
        sourceTask = new MySqlConnectorTask();
        break;
      case MYSQL_BATCH:
        sourceTask = new MysqlBatchSourceTask();
        break;
      case ORACLE:
        sourceTask = new OracleSourceTask();
        break;
      case SQL_SERVER:
        sourceTask = new SqlServerSourceTask();
        break;
      case FILE_SYSTEM:
        sourceTask = new FileSystemSourceTask();
        break;
      case COUCHBASE:
        sourceTask = new CouchbaseSourceTask();
        break;
      case FTP:
        sourceTask = new FileSystemSourceTask();
        break;
      case POSTGRES_BATCH:
        sourceTask = new PostgresSourceTask();
        break;
      case POSTGRES:
        sourceTask = new PostgresConnectorTask();
        break;
      case KAFKA:
        sourceTask = new KafkaSourceTask();
        break;
      default:
        throw new IllegalArgumentException("Source Type not support yet");
    }
    sourceTask.initialize(context);
    return sourceTask;
  }
}