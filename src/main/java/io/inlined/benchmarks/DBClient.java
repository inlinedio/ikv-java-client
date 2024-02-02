package io.inlined.benchmarks;

import java.util.Map;

public interface DBClient {
  // init methods are implementation dependent
  void startup();

  void shutdown();

  byte[] getValue(byte[] key, String fieldName, byte[] fieldNameUtf8Bytes);

  void setValues(byte[] key, Map<String, byte[]> fieldValues);
}
