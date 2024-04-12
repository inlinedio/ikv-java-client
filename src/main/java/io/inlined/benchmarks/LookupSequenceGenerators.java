package io.inlined.benchmarks;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.IntStream;

// Returns an iterator which can be used
// to create sample-ids for lookups
public class LookupSequenceGenerators {
  private static final Random RANDOM = new Random();

  public static Iterator<Integer> sequential(int numSamples) {
    return IntStream.range(0, numSamples).iterator();
  }

  public static Iterator<Integer> random(int numSamples) {
    return IntStream.range(0, numSamples).map(x -> RANDOM.nextInt(0, numSamples)).iterator();
  }
}
