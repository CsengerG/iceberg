Simple README with instructions of my iteration.
I hate that the spotless check always reformats my
comments in code, hence the Markdown file.

My process:

1. Make a change
2. Apply formatting
 ```
   ./gradlew spotlessApply
 ```
3. Compile
```
   ./gradlew build -x test -x integrationTest
```
4. Execute
```
./gradlew \
   -DsparkVersions=3.5 \
   :iceberg-spark:iceberg-spark-3.5_2.12:jmh \
   -PjmhIncludeRegex=CsengerGBenchmark \
   -PjmhOutputPath=benchmark/csengerg-benchmark-result.txt
```
   

