package io.inlined.benchmarks;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.inlined.benchmarks.clients.IKVSingleGetDBClient;
import io.inlined.benchmarks.clients.RedisSingleGetDBClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Java executable which runs benchmarks on various database(s) based on input parameters. */
public class BenchRunner {
  private static final Logger LOGGER = LogManager.getLogger(BenchRunner.class);

  /**
   * Driver method.
   *
   * <pre>
   *     Usage examples -
   *     [1] With writes (ex. on redis):
   *     java -cp /path/to/jar io.inlined.benchmarks.BenchRunner
   *     redis clusterUrl:foo,init_writes:1,num_samples:1000,num_fields:3,value_size:50
   *
   *     [2] No writes (ex. on IKV):
   *     java -cp /path/to/jar io.inlined.benchmarks.BenchRunner
   *     ikv accountid:foo,accountpasskey:bar,num_samples:1000,num_fields:3,value_size:50
   * </pre>
   *
   * @param args list- arg[0] - redis, ikv, dynamodb (database to benchmark) arg[1] - {@link
   *     BenchmarkParams} string param list like - key1:value1,key2:value2,key3:value3
   */
  public static void main(String[] args) {
    String dbType = args[0].toLowerCase();
    BenchmarkParams benchmarkParams = new BenchmarkParams(args[1]);
    SingleGetDBClient singleGetDBClient;
    switch (dbType) {
      case "ikv" -> {
        LOGGER.info("Executing benchmarks on IKV, params: {}", benchmarkParams);
        String accountid = benchmarkParams.getStringParameter("accountid").get();
        String accountpasskey = benchmarkParams.getStringParameter("accountpasskey").get();
        singleGetDBClient = new IKVSingleGetDBClient(accountid, accountpasskey);
      }
      case "redis" -> {
        LOGGER.info("Executing benchmarks on Redis, params: {}", benchmarkParams);
        String clusterUrl = benchmarkParams.getStringParameter("clusterUrl").get();
        singleGetDBClient = new RedisSingleGetDBClient(clusterUrl);
      }
      case "dynamodb" -> {
        LOGGER.info("Executing benchmarks on DynamoDB, params: {}", benchmarkParams);
        throw new UnsupportedOperationException("todo");
      }
      default -> {
        LOGGER.error("Unsupported database type: {}", dbType);
        return;
      }
    }

    // drive
    BenchRunner benchRunner = new BenchRunner(singleGetDBClient, benchmarkParams);
    benchRunner.startup();
    benchRunner.initializeWithWrites();
    benchRunner.runSingleGetBenchmark();
    benchRunner.shutdown();
  }

  private final SingleGetDBClient _dbClient;
  private final ArrayList<String> _fieldNames;
  private final int _numSamples;
  private final int _numFields;
  private final int _valueSize;
  private final boolean _initializeWithWrites;

  public BenchRunner(SingleGetDBClient dbClient, BenchmarkParams params) {
    _dbClient = dbClient;
    _numSamples = params.getIntegerParameter("num_samples").get();
    _numFields = params.getIntegerParameter("num_fields").get();
    _valueSize = params.getIntegerParameter("value_size").get();
    _initializeWithWrites = params.getIntegerParameter("init_writes").isPresent();
    _fieldNames = new ArrayList<>(_numFields);
    for (int i = 0; i < _numFields; i++) {
      _fieldNames.add(String.format("field_%d", i));
    }
  }

  public void startup() {
    _dbClient.startup();
    LOGGER.info("DBClient startup complete.");
  }

  public void shutdown() {
    _dbClient.shutdown();
    LOGGER.info("DBClient shutdown complete.");
  }

  public void runSingleGetBenchmark() {
    // single warmup iteration
    Instant start = Instant.now();
    runSingleGetBenchmarkImpl(null);
    LOGGER.info(
        "runSingleGetBenchmark warmup done in: {} sec",
        Duration.between(start, Instant.now()).toSeconds());

    // instrumented run
    Histogram histogram = new Histogram("", _numSamples);
    runSingleGetBenchmarkImpl(histogram);
    LOGGER.info("runSingleGetBenchmark finished. Results- {}", histogram.toString());
  }

  private void runSingleGetBenchmarkImpl(@Nullable Histogram histogram) {
    KVGeneratorV2 kvGeneratorV2 = new KVGeneratorV2(_numSamples, _valueSize);
    for (int sample = 0; sample < _numSamples; sample++) {
      int fid = sample % _numFields;
      String fieldName = _fieldNames.get(fid);
      byte[] key = kvGeneratorV2.key(sample);
      byte[] value = KVGeneratorV2.createPseudoRandomBytes(_valueSize, sample * fid);

      // instrumented db fetch call
      Instant a = Instant.now();
      byte[] fetchedValue = _dbClient.getValue(key, fieldName, fieldName.getBytes());
      Instant b = Instant.now();

      Preconditions.checkArgument(Arrays.equals(value, fetchedValue));
      if (histogram != null) {
        // track in nanoseconds
        histogram.captureLatency(Duration.between(a, b).toNanos());
      }
    }
  }

  public void initializeWithWrites() {
    if (!_initializeWithWrites) {
      LOGGER.info("Skipping initialization with writes.");
      return;
    }

    Instant start = Instant.now();
    LOGGER.info("Starting initialization with writes.");

    KVGeneratorV2 kvGeneratorV2 = new KVGeneratorV2(_numSamples, _valueSize);
    for (int sample = 0; sample < _numSamples; sample++) {
      // construct document
      Map<String, byte[]> document = Maps.newHashMapWithExpectedSize(_numFields);
      for (int fid = 0; fid < _numFields; fid++) {
        String fieldName = _fieldNames.get(fid);
        byte[] value = KVGeneratorV2.createPseudoRandomBytes(_valueSize, sample * fid);
        document.put(fieldName, value);
      }

      // write to db client
      byte[] key = kvGeneratorV2.key(sample);
      _dbClient.setValues(key, document);
    }

    LOGGER.info(
        "Write initialization done in: {} sec", Duration.between(start, Instant.now()).toSeconds());
  }
}
