# spark.mergesort.example
An example of how to do a merge sort

## Generation Data
spark-submit --class com.cloudera.sa.spark.mergesort.example.DataGenerator \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
MergeExample.jar \
l \
10000 \
5 \
10 \
gen_data \
/Users/ted.malaska/Documents/workspace/github/spark.mergesort.example/hive/gen_data

## Initial Bucket And Sort
spark-submit --class com.cloudera.sa.spark.mergesort.example.InitialBucketAndSort \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
MergeExample.jar \
l \
10 \
gen_data \
bs_data \
/Users/ted.malaska/Documents/workspace/github/spark.mergesort.example/hive/bs_data

## Generation Data New Set
spark-submit --class com.cloudera.sa.spark.mergesort.example.DataGenerator \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
MergeExample.jar \
l \
1000 \
5 \
10 \
gen_data_2 \
/Users/ted.malaska/Documents/workspace/github/spark.mergesort.example/hive/gen_data_2

## Bucketed Sorted Merge
spark-submit --class com.cloudera.sa.spark.mergesort.example.BucketedSortedMerge \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
MergeExample.jar \
l \
10 \
bs_data \
gen_data_2 \
/Users/ted.malaska/Documents/workspace/github/spark.mergesort.example/hive/tmp \
merged_table \
/Users/ted.malaska/Documents/workspace/github/spark.mergesort.example/hive/merged_data

## View the Data
spark-submit --class com.cloudera.sa.spark.mergesort.example.ViewData \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
MergeExample.jar \
l /
merged_table \
100

