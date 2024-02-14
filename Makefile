build:
	./gradlew spotlessApply
	./gradlew build -x test -x integrationTest

jmh:
	./gradlew \
	-DsparkVersions=3.5 \
	:iceberg-spark:iceberg-spark-3.5_2.12:jmh \
	-PjmhIncludeRegex=CsengerGBenchmark \
	-PjmhOutputPath=benchmark/csengerg-benchmark-result.txt
