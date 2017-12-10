Performance Test for `Spark` job with `Kudu` including reading, writing and updating data.

## Run Project

1. compile `mvn package`
2. run **spark-submit** `sudo -Hu hdfs spark2-submit --master yarn --deploy-mode client --class com.zmousa.persist.PerformanceJob --num-executors 2 --driver-cores 1 --driver-memory 2g --executor-memory 2g --executor-cores 1 --queue default /test/spark-on-kudu-performance-1.0-SNAPSHOT.jar 1369084798 1000000`
3. Parameters (first generated row index, total generated rows).
