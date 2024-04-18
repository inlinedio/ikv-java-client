package io.inlined.benchmarks;

import com.google.common.collect.Lists;
import io.inlined.benchmarks.ycsbutils.ScrambledZipfianGenerator;
import io.inlined.benchmarks.ycsbutils.ZipfianGenerator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

// Returns an iterator which can be used
// to create sample-ids for lookups
public class LookupSequenceGenerators {
  public static Iterator<Integer> singleKeyGenerator(String type, int numSamples) {
    switch (type) {
      case "random" -> {
        return random(numSamples);
      }
      case "sequential" -> {
        return sequential(numSamples);
      }
      case "zipfian" -> {
        return zipfian(numSamples);
      }
      case "scrambledzipfian" -> {
        return scrambledZipfian(numSamples);
      }
      default -> throw new UnsupportedOperationException("unknown sequence type");
    }
  }

  public static Iterator<List<Integer>> batchKeysGenerator(
      String type, int numSamples, int batchSize) {
    switch (type) {
      case "random" -> {
        return batchedRandom(numSamples, batchSize);
      }
      case "sequential" -> {
        return batchedSequential(numSamples, batchSize);
      }
      case "zipfian" -> {
        return batchedZipfian(numSamples, batchSize);
      }
      case "scrambledzipfian" -> {
        return batchedScrambledZipfian(numSamples, batchSize);
      }
      default -> throw new UnsupportedOperationException("unknown sequence type");
    }
  }

  /** Sequential key generation, ex. 0,1,2,...numSamples */
  private static Iterator<Integer> sequential(int numSamples) {
    return IntStream.range(0, numSamples).iterator();
  }

  private static Iterator<List<Integer>> batchedSequential(int numSamples, int batchSize) {
    int numBatches = numSamples / batchSize;
    return IntStream.range(0, numBatches)
        .mapToObj(
            batchId ->
                IntStream.range(batchId * batchSize, (batchId + 1) * batchSize).boxed().toList())
        .iterator();
  }

  /** Uniformly random key generation */
  private static Iterator<Integer> random(int numSamples) {
    return IntStream.range(0, numSamples)
        .map(x -> ThreadLocalRandom.current().nextInt(0, numSamples))
        .iterator();
  }

  private static Iterator<List<Integer>> batchedRandom(int numSamples, int batchSize) {
    int numBatches = numSamples / batchSize;
    return IntStream.range(0, numBatches)
        .mapToObj(
            batchId ->
                IntStream.range(0, batchSize)
                    .map(x -> ThreadLocalRandom.current().nextInt(0, numSamples))
                    .boxed()
                    .toList())
        .iterator();
  }

  /** Clustered Zipfian (keys with high frequency will be closer) key generation */
  private static Iterator<Integer> zipfian(int numSamples) {
    ZipfianGenerator zipfianGenerator = new ZipfianGenerator(numSamples);
    return IntStream.range(0, numSamples)
        .map(x -> zipfianGenerator.nextValue().intValue())
        .iterator();
  }

  private static Iterator<List<Integer>> batchedZipfian(int numSamples, int batchSize) {
    ZipfianGenerator zipfianGenerator = new ZipfianGenerator(numSamples);
    int numBatches = numSamples / batchSize;
    return IntStream.range(0, numBatches)
        .mapToObj(
            batchId ->
                IntStream.range(0, batchSize)
                    .map(x -> zipfianGenerator.nextValue().intValue())
                    .boxed()
                    .toList())
        .iterator();
  }

  /** Scrambled Zipfian (keys with high frequency will be scrambled) key generation */
  private static Iterator<Integer> scrambledZipfian(int numSamples) {
    ScrambledZipfianGenerator zipfianGenerator = new ScrambledZipfianGenerator(numSamples);
    return IntStream.range(0, numSamples)
        .map(x -> zipfianGenerator.nextValue().intValue())
        .iterator();
  }

  private static Iterator<List<Integer>> batchedScrambledZipfian(int numSamples, int batchSize) {
    ScrambledZipfianGenerator zipfianGenerator = new ScrambledZipfianGenerator(numSamples);
    int numBatches = numSamples / batchSize;
    return IntStream.range(0, numBatches)
        .mapToObj(
            batchId ->
                IntStream.range(0, batchSize)
                    .map(x -> zipfianGenerator.nextValue().intValue())
                    .boxed()
                    .toList())
        .iterator();
  }

  // For ad hoc testing.
  public static void main(String[] args) {
    int numSamples = 100;
    int batchSize = 5;

    System.out.println("Zipfian: " + Lists.newArrayList(zipfian(numSamples)));
    System.out.println(
        "Batched Zipfian: " + Lists.newArrayList(batchedZipfian(numSamples, batchSize)));

    System.out.println("Scrambled Zipfian: " + Lists.newArrayList(scrambledZipfian(numSamples)));
  }
}
