package io.inlined.benchmarks.clients;

import com.google.common.collect.Maps;
import io.inlined.benchmarks.DBClient;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSingleGetDBClient implements DBClient {
  private static final Logger LOGGER = LogManager.getLogger(RedisSingleGetDBClient.class);

  private final Set<HostAndPort> _jedisClusterNodes;
  private final int _maxConnections;

  private volatile JedisCluster _jedisCluster;

  // private volatile Jedis _jedis;  // for single node local benchmark

  public RedisSingleGetDBClient(String clusterUrls, int maxConnections) {
    _jedisClusterNodes = new HashSet<>();
    for (String clusterUrl : clusterUrls.split("\\|")) {
      _jedisClusterNodes.add(new HostAndPort(clusterUrl, 6379));
    }
    _maxConnections = maxConnections;
  }

  @Override
  public void startup() {
    // single node testing:
    // JedisPool jedisPool = new JedisPool("localhost", 6379);
    // _jedis = jedisPool.getResource();

    try {
      JedisPoolConfig cfg = new JedisPoolConfig();
      cfg.setMaxTotal(_maxConnections); // 2x num concurrent benchmarking threads
      cfg.setMaxIdle(_maxConnections);

      LOGGER.info("JedisClusterNodes: {}", _jedisClusterNodes);
      _jedisCluster = new JedisCluster(_jedisClusterNodes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    // _jedis.close();
    _jedisCluster.close();
    _jedisCluster = null;
  }

  @Override
  public byte[] getValue(byte[] key, String unused, byte[] fieldNameUtf8Bytes) {
    return _jedisCluster.hget(key, fieldNameUtf8Bytes);
  }

  @Override
  public void setValues(byte[] key, Map<String, byte[]> fieldValues) {
    // convert to Map<byte[], byte[]> values
    Map<byte[], byte[]> copy = Maps.newHashMapWithExpectedSize(fieldValues.size());
    for (Map.Entry<String, byte[]> entry : fieldValues.entrySet()) {
      copy.put(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue());
    }
    _jedisCluster.hset(key, copy);
  }
}
