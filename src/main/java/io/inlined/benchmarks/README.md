# Instructions on how to use benchmark tooling

### Build
Execute `./gradlew build` in project root directory and find the executable in
`build/libs`.

### Execute
`java -cp /path/to/jar io.inlined.benchmarks.runner.MultiThreadedRunner "{redis|ikv}" "params"`.
Where `params` are of the format "k1:v1,k2:v2" ex "threads:2,max_qps:10000". For full param reference list,
see javadocs for `MultiThreadedRunner`.

### Code Layout
- io.inlined.benchmarks.clients - Database specific adapter implementations
- io.inlined.benchmarks.runner - driver code (executable)
- io.inlined.benchmarks - instrumentation like histograms, data generators, etc.
