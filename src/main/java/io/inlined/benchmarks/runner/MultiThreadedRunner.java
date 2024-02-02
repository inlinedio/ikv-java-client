package io.inlined.benchmarks.runner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import io.inlined.benchmarks.BenchmarkParams;
import io.inlined.benchmarks.DBClient;
import io.inlined.benchmarks.Histogram;
import io.inlined.benchmarks.KVGeneratorV2;
import io.inlined.benchmarks.clients.IKVSingleGetDBClient;
import io.inlined.benchmarks.clients.RedisSingleGetDBClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MultiThreadedRunner {
  private static final Logger LOGGER = LogManager.getLogger(MultiThreadedRunner.class);

  /**
   * Driver method.
   *
   * <p>Uses multiple parallel threads which make sequential blocking read queries to the underlying
   * database.
   *
   * <pre>
   * Argument reference:
   * 1) num_samples: (int, required) number of k-v pairs / documents
   * 2) num_fields: (int, required) number of inner fields/attributes per k-v pair/document
   * 3) value_size: (int, required) size of value for each inner field in bytes
   * 4) init_writes: (int, optional) specify any value to initialize db with writes, or omit to
   * skip write initialization
   * 5) max_runs: (int, optional) number of loops over num_samples for read benchmark. Use if
   * running without duration_sec param.
   * 6) duration_sec: (int, optional) benchmark time window
   * 7) threads: (int, optional) number of parallel client threads to launch for benchmarking
   * 8) max_qps: (int, optional) max QPS (among all threads) or load for benchmark
   * 9) warmups: (int, optional) number of read traffic warmups before actual instrumented run.
   * 10) accountid: (string, required for ikv) IKV account-id
   * 11) accountpasskey: (string, required for ikv) IKV account-passkey
   * 12) storename: (string, required for ikv) IKV store name
   * 13) clusterUrls: (strings concatenated with '|', required for redis) shard URLs. Tool assumes
   * each shard uses port 6379.
   *
   *     Usage examples -
   *     [1] On Redis
   *     java -cp /path/to/jar io.inlined.benchmarks.runner.MultiThreadedRunner
   *     "redis" "clusterUrls:foo1|foo2|foo3,init_writes:1,num_samples:1000,num_fields:3,value_size:50,duration_sec:10,threads:2"
   *
   *     [2] On IKV
   *     java -cp /path/to/jar io.inlined.benchmarks.runner.MultiThreadedRunner
   *     "ikv" "accountid:foo,accountpasskey:bar,storename:baz,init_writes:1,num_samples:1000,num_fields:3,value_size:50,duration_sec:10,threads:2"
   *
   * </pre>
   *
   * @param args list- arg[0] - redis, ikv, dynamodb (database to benchmark) arg[1] - {@link
   *     BenchmarkParams} string param list like - key1:value1,key2:value2,key3:value3
   */
  public static void main(String[] args) {
    String dbType = args[0].toLowerCase();
    BenchmarkParams benchmarkParams = new BenchmarkParams(args[1]);
    DBClient DBClient;
    switch (dbType) {
      case "ikv" -> {
        String accountid = benchmarkParams.getStringParameter("accountid").get();
        String accountpasskey = benchmarkParams.getStringParameter("accountpasskey").get();
        String storeName = benchmarkParams.getStringParameter("storename").get();
        DBClient = new IKVSingleGetDBClient(accountid, accountpasskey, storeName);
      }
      case "redis" -> {
        String clusterUrls = benchmarkParams.getStringParameter("clusterUrls").get();
        int threads = benchmarkParams.getIntegerParameter("threads").orElse(1);
        DBClient = new RedisSingleGetDBClient(clusterUrls, 2 * threads);
      }
      default -> throw new UnsupportedOperationException("unsupported database type");
    }

    // execute
    new MultiThreadedRunner(DBClient, benchmarkParams).run();
  }

  private final DBClient _dbClient;
  private final ArrayList<String> _fieldNames;
  private final KVGeneratorV2 _kvGenerator;

  private final int _numSamples;
  private final int _numFields;
  private final int _valueSize;
  private final boolean _initWrites;

  private final int _maxRuns;
  private final int _threads;
  private final double _maxRate;
  private final Duration _benchmarkDuration;

  public MultiThreadedRunner(DBClient dbClient, BenchmarkParams params) {
    _dbClient = dbClient;
    _numSamples = params.getIntegerParameter("num_samples").get();
    _numFields = params.getIntegerParameter("num_fields").get();
    _valueSize = params.getIntegerParameter("value_size").get();
    _initWrites = params.getIntegerParameter("init_writes").isPresent();
    _fieldNames = new ArrayList<>(_numFields);
    for (int i = 0; i < _numFields; i++) {
      _fieldNames.add(String.format("field_%d", i));
    }
    _kvGenerator = new KVGeneratorV2(_numSamples);

    _maxRuns = params.getIntegerParameter("max_runs").orElse(Integer.MAX_VALUE);
    _threads = params.getIntegerParameter("threads").orElse(1);
    _maxRate = params.getIntegerParameter("max_qps").orElse(Integer.MAX_VALUE);
    _benchmarkDuration =
        Duration.ofSeconds(params.getIntegerParameter("duration_sec").orElse(Integer.MAX_VALUE));
  }

  public void run() {
    _dbClient.startup();
    LOGGER.info("DBClient startup complete.");

    initWrites();

    execGetBenchmark();

    _dbClient.shutdown();
    LOGGER.info("DBClient shutdown complete.");
  }

  public void execGetBenchmark() {
    // single warmup iteration
    Instant start = Instant.now();
    singleThreadedGetBenchmark(
        new Histogram("warmup"), RateLimiter.create(Double.MAX_VALUE), Duration.ofDays(365), 1);
    LOGGER.info(
        "execGetBenchmark warmup done in: {} sec",
        Duration.between(start, Instant.now()).toSeconds());

    // instrumented run
    Histogram histogram = new Histogram("");

    // tasks and results
    List<CompletableFuture<Void>> futures = new ArrayList<>(_threads);
    final List<GetBenchmarkStats> indivStats = new ArrayList<>(_threads);

    for (int thread = 0; thread < _threads; thread++) {
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                GetBenchmarkStats s =
                    singleThreadedGetBenchmark(
                        histogram,
                        RateLimiter.create(_maxRate / _threads),
                        _benchmarkDuration,
                        _maxRuns);
                indivStats.add(s);
              }));
    }

    // wait
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    // aggregate qps
    double aggregatedQps = 0;
    for (GetBenchmarkStats s : indivStats) {
      double qps = (double) s._numRequests / s._executionTimeSeconds;
      aggregatedQps += qps;
    }

    LOGGER.info(
        "execGetBenchmark finished. QPS- {} Results - {}", aggregatedQps, histogram.toString());
  }

  private GetBenchmarkStats singleThreadedGetBenchmark(
      Histogram histogram, RateLimiter rateLimiter, Duration experimentDuration, int maxRuns) {
    Instant benchmarkStartInstant = Instant.now();
    Instant deadline = benchmarkStartInstant.plus(experimentDuration);

    // stats
    long numRequests = 0;

    benchLoop:
    for (int runId = 0; runId < maxRuns; runId++) {
      for (int sample = 0; sample < _numSamples; sample++) {

        // end time check
        if (Instant.now().isAfter(deadline)) {
          break benchLoop;
        }

        int fid = sample % _numFields;
        String fieldName = _fieldNames.get(fid);
        byte[] key = _kvGenerator.key(sample);
        byte[] value = KVGeneratorV2.createPseudoRandomBytes(_valueSize, sample * fid);

        // wait for permit
        rateLimiter.acquire();

        // instrumented db fetch call
        Instant a = Instant.now();
        byte[] fetchedValue = _dbClient.getValue(key, fieldName, fieldName.getBytes());
        Instant b = Instant.now();

        Preconditions.checkArgument(Arrays.equals(value, fetchedValue));
        numRequests++;
        histogram.captureLatency(Duration.between(a, b).toNanos());
      }
    }

    // Report stats
    GetBenchmarkStats stats = new GetBenchmarkStats();
    stats._numRequests = numRequests;
    stats._executionTimeSeconds =
        Duration.between(benchmarkStartInstant, Instant.now()).toSeconds();

    return stats;
  }

  private void initWrites() {
    if (!_initWrites) {
      LOGGER.info("Skipping initialization with writes.");
      return;
    }

    LOGGER.info("initWrites: Started.");

    // 16 parallel writer threads
    List<CompletableFuture<Void>> futures = new ArrayList<>(16);

    for (int start = 0; start < _numSamples; ) {
      int start_sample = start;
      int end_sample = start_sample + (_numSamples / 16);

      futures.add(
          CompletableFuture.runAsync(
              () -> {
                for (int sample = start_sample;
                    sample < Math.min(end_sample, _numSamples);
                    sample++) {
                  // construct document
                  Map<String, byte[]> document = Maps.newHashMapWithExpectedSize(_numFields);
                  for (int fid = 0; fid < _numFields; fid++) {
                    String fieldName = _fieldNames.get(fid);
                    byte[] value = KVGeneratorV2.createPseudoRandomBytes(_valueSize, sample * fid);
                    document.put(fieldName, value);
                  }

                  // write to db client
                  byte[] key = _kvGenerator.key(sample);
                  _dbClient.setValues(key, document);
                }
              }));

      start = end_sample;
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    LOGGER.info("initWrites: Finished.");
  }

  // POJO holder to enable benchmarking threads to
  // report stats about the run.
  private static class GetBenchmarkStats {
    long _executionTimeSeconds;
    long _numRequests;
  }
}
