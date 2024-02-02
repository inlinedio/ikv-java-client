package io.inlined;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class NoopBenchmark {
  @Setup(Level.Invocation)
  public void setup() {}

  @Benchmark
  public void ikvBenchmark(Blackhole bh) {}

  @Benchmark
  public void redisBenchmark(Blackhole bh) {}
}
