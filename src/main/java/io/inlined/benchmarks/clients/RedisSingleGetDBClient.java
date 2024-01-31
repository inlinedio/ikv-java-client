package io.inlined.benchmarks.clients;

import com.google.common.collect.Maps;
import io.inlined.benchmarks.SingleGetDBClient;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisSingleGetDBClient implements SingleGetDBClient {
  private final Set<HostAndPort> _jedisClusterNodes;
  private volatile JedisCluster _jedisCluster;

  // private volatile Jedis _jedis;  // for single node local benchmark

  public RedisSingleGetDBClient(String clusterUrl) {
    _jedisClusterNodes = new HashSet<>();
    _jedisClusterNodes.add(new HostAndPort(clusterUrl, 6379));
  }

  @Override
  public void startup() {
    // single node testing:
    // JedisPool jedisPool = new JedisPool("localhost", 6379);
    // _jedis = jedisPool.getResource();

    try {
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
