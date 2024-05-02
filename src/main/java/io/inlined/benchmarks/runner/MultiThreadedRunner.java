package io.inlined.benchmarks.runner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import io.inlined.benchmarks.*;
import io.inlined.benchmarks.clients.IKVSingleGetDBClient;
import io.inlined.benchmarks.clients.RedisSingleGetDBClient;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
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
   * 5) max_runs: (int, optional/required) number of loops over num_samples for read benchmark. Required if
   * running without duration_sec param, else omit.
   * 6) duration_sec: (int, optional) benchmark time window
   * 7) threads: (int, optional) number of parallel client threads to launch for benchmarking
   * 8) max_qps: (int, optional) max QPS (among all threads) or load for benchmark
   * 9) warmups: (int, optional) number of read traffic warmups before actual instrumented run.
   * 10) accountid: (string, required for ikv) IKV account-id
   * 11) accountpasskey: (string, required for ikv) IKV account-passkey
   * 12) storename: (string, required for ikv) IKV store name
   * 13) clusterUrls: (strings concatenated with '|', required for redis) shard URLs. Tool assumes
   * each shard uses port 6379.
   * 14) sequenceType: key iteration order for lookups. Valid values - random,sequential,zipfian,scrambledzipfian.
   * Defaults to random.
   * 15) mode: single or batch get mode. Values - single,batch. Defaults to single.
   * 16) batch_size: (int, optional/required) when mode is batch
   *
   *     Usage examples -
   *     [1] On Redis
   *     java -cp /path/to/jar io.inlined.benchmarks.runner.MultiThreadedRunner
   *     "redis" "clusterUrls:foo1|foo2|foo3,init_writes:1,num_samples:1000,num_fields:3,value_size:50,duration_sec:10,threads:2"
   *
   *     [2] On IKV
   *     java -cp /path/to/jar io.inlined.benchmarks.runner.MultiThreadedRunner
   *     "ikv" "accountid:foo,accountpasskey:bar,storename:baz,primarykeyfieldname:userid,mountdir:/path/to/dir;init_writes:1,num_samples:1000,num_fields:3,value_size:50,duration_sec:10,threads:2"
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
        String primaryKeyFieldName =
            benchmarkParams.getStringParameter("primarykeyfieldname").get();
        String mountdir = benchmarkParams.getStringParameter("mountdir").get();
        DBClient =
            new IKVSingleGetDBClient(
                false, accountid, accountpasskey, storeName, primaryKeyFieldName, mountdir);
      }
        // write directly into on-host index
      case "directikv" -> {
        String accountid = benchmarkParams.getStringParameter("accountid").get();
        String accountpasskey = benchmarkParams.getStringParameter("accountpasskey").get();
        String storeName = benchmarkParams.getStringParameter("storename").get();
        String primaryKeyFieldName =
            benchmarkParams.getStringParameter("primarykeyfieldname").get();
        String mountdir = benchmarkParams.getStringParameter("mountdir").get();
        DBClient =
            new IKVSingleGetDBClient(
                true, accountid, accountpasskey, storeName, primaryKeyFieldName, mountdir);
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
  private final KVGenerator _kvGenerator;

  private final int _numSamples;
  private final int _numFields;
  private final int _valueSize;
  private final boolean _initWrites;

  private final int _maxRuns;
  private final int _numWarmups;
  private final int _threads;
  private final double _maxRate;
  private final int _batchSize;
  private final Duration _benchmarkDuration;
  private final String _sequenceType;
  private final String _mode;

  public MultiThreadedRunner(DBClient dbClient, BenchmarkParams params) {
    _dbClient = dbClient;
    _numSamples = params.getIntegerParameter("num_samples").get();
    _numFields = params.getIntegerParameter("num_fields").get();
    _valueSize = params.getIntegerParameter("value_size").get();
    _initWrites = params.getIntegerParameter("init_writes").isPresent();
    _mode = params.getStringParameter("mode").orElse("single");
    _fieldNames = new ArrayList<>(_numFields);
    for (int i = 0; i < _numFields; i++) {
      _fieldNames.add(String.format("field_%d", i));
    }
    _kvGenerator = new KVGenerator(_numSamples);

    _maxRuns = params.getIntegerParameter("max_runs").orElse(Integer.MAX_VALUE);
    _numWarmups = params.getIntegerParameter("warmups").orElse(1);
    _threads = params.getIntegerParameter("threads").orElse(1);
    _maxRate = params.getIntegerParameter("max_qps").orElse(Integer.MAX_VALUE);
    _batchSize = params.getIntegerParameter("batch_size").orElse(_numSamples);
    _benchmarkDuration =
        Duration.ofSeconds(params.getIntegerParameter("duration_sec").orElse(Integer.MAX_VALUE));
    _sequenceType = params.getStringParameter("sequenceType").orElse("random");
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
    for (int i = 0; i < _numWarmups; i++) {
      singleThreadedSingleGetBenchmark(
          new Histogram("warmup"), RateLimiter.create(Double.MAX_VALUE), Duration.ofDays(365), 1);
    }
    LOGGER.info("Warmup(s) done in: {} sec", Duration.between(start, Instant.now()).toSeconds());

    // instrumented run
    Histogram histogram = new Histogram("");

    // tasks and results
    List<CompletableFuture<Void>> futures = new ArrayList<>(_threads);
    final List<GetBenchmarkStats> indivStats = new ArrayList<>(_threads);

    for (int thread = 0; thread < _threads; thread++) {
      if ("single".equals(_mode)) {
        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  GetBenchmarkStats s =
                      singleThreadedSingleGetBenchmark(
                          histogram,
                          RateLimiter.create(_maxRate / _threads),
                          _benchmarkDuration,
                          _maxRuns);
                  indivStats.add(s);
                }));
      } else if ("batch".equals(_mode)) {
        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  GetBenchmarkStats s =
                      singleThreadedBatchGetBenchmark(
                          histogram,
                          RateLimiter.create(_maxRate / _threads),
                          _benchmarkDuration,
                          _maxRuns);
                  indivStats.add(s);
                }));
      }
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
        "execGetBenchmark finished. QPS- {} Results (Nano-sec) - {}",
        aggregatedQps,
        histogram.toString());
  }

  private GetBenchmarkStats singleThreadedSingleGetBenchmark(
      Histogram histogram, RateLimiter rateLimiter, Duration experimentDuration, int maxRuns) {
    Instant benchmarkStartInstant = Instant.now();
    Instant deadline = benchmarkStartInstant.plus(experimentDuration);

    // stats
    long numRequests = 0;

    benchLoop:
    for (int runId = 0; runId < maxRuns; runId++) {
      Iterator<Integer> sequence =
          LookupSequenceGenerators.singleKeyGenerator(_sequenceType, _numSamples);
      while (sequence.hasNext()) {
        int sample = sequence.next();

        // time check
        if (Instant.now().isAfter(deadline)) {
          break benchLoop;
        }

        int fid = sample % _numFields;
        String fieldName = _fieldNames.get(fid);
        byte[] key = _kvGenerator.key(sample);
        byte[] value = KVGenerator.createPseudoRandomBytes(_valueSize, sample * fid);

        // wait for permit
        rateLimiter.acquire();

        // instrumented db fetch call
        Instant a = Instant.now();
        byte[] fetchedValue = _dbClient.getValue(key, fieldName, fieldName.getBytes());
        Instant b = Instant.now();

        Preconditions.checkArgument(
            Arrays.equals(value, fetchedValue),
            String.format("Mismatch for key=%s", new String(key)));
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

  private GetBenchmarkStats singleThreadedBatchGetBenchmark(
      Histogram histogram, RateLimiter rateLimiter, Duration experimentDuration, int maxRuns) {
    Instant benchmarkStartInstant = Instant.now();
    Instant deadline = benchmarkStartInstant.plus(experimentDuration);

    // stats
    long numRequests = 0;

    benchLoop:
    for (int runId = 0; runId < maxRuns; runId++) {
      Iterator<List<Integer>> sequence =
          LookupSequenceGenerators.batchKeysGenerator(_sequenceType, _numSamples, _batchSize);
      while (sequence.hasNext()) {
        List<Integer> samples = sequence.next();

        // time check
        if (Instant.now().isAfter(deadline)) {
          break benchLoop;
        }

        // TODO- fix.
        int fid = ThreadLocalRandom.current().nextInt(0, _numFields);
        String fieldName = _fieldNames.get(fid);

        Iterator<byte[]> keys = samples.stream().map(_kvGenerator::key).iterator();
        List<byte[]> values =
            samples.stream()
                .map(sample -> KVGenerator.createPseudoRandomBytes(_valueSize, sample * fid))
                .toList();

        // wait for permit
        rateLimiter.acquire();

        // instrumented db fetch call
        Instant a = Instant.now();
        Iterator<byte[]> fetchedValues =
            _dbClient.multiGetValueForKeys(keys, fieldName, fieldName.getBytes());
        Instant b = Instant.now();

        for (byte[] expectedValue : values) {
          Preconditions.checkArgument(Arrays.equals(expectedValue, fetchedValues.next()));
        }

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
                    byte[] value = KVGenerator.createPseudoRandomBytes(_valueSize, sample * fid);
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

    _dbClient.flushValues();

    LOGGER.info("initWrites: Finished.");
  }

  // POJO holder to enable benchmarking threads to
  // report stats about the run.
  private static class GetBenchmarkStats {
    long _executionTimeSeconds;
    long _numRequests;
  }
}
