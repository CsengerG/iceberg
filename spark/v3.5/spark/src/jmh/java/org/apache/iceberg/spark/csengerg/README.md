Simple README with instructions of my iteration.
I hate that the spotless check always reformats my
comments in code, hence the Markdown file.

### Workflow

1. Make a change
2. Apply formatting
 ```
   ./gradlew spotlessApply
 ```
3. Compile
```
   ./gradlew build -x test -x integrationTest
```

### Run benchmarks

#### Execute Sequential Read Benchmark
```
./gradlew \
   -DsparkVersions=3.5 \
   :iceberg-spark:iceberg-spark-3.5_2.12:jmh \
   -PjmhIncludeRegex=SequentialReadBenchmark \
   -PjmhOutputPath=benchmark/csv-sequential-read-benchmark-result.txt
```

#### Execute Parquet Read Benchmarks

```
./gradlew \
   -DsparkVersions=3.5 \
   :iceberg-spark:iceberg-spark-3.5_2.12:jmh \
   -PjmhIncludeRegex=ParquetReadBenchmark \
   -PjmhOutputPath=benchmark/parquet-read-benchmark-result.txt
```
