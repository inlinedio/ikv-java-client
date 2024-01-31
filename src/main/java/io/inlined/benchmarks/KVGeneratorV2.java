package io.inlined.benchmarks;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class KVGeneratorV2 {
  private final ArrayList<byte[]> _keys;

  public KVGeneratorV2(int samples, int valueSize) {
    _keys = new ArrayList<>(samples);
    for (int i = 0; i < samples; i++) {
      byte[] key = String.format("{key}-%d", i).getBytes(StandardCharsets.UTF_8);
      byte[] value = createPseudoRandomBytes(valueSize, i);
      _keys.add(key);
    }
  }

  public byte[] key(int sample) {
    return _keys.get(sample);
  }

  // Should be used to create values
  // Ex. seed = sample * field-id
  public static byte[] createPseudoRandomBytes(int length, int seed) {
    byte[] result = new byte[length];
    Arrays.fill(result, (byte) seed);
    return result;
  }
}
