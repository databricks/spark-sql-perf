# SparkR \*apply() benchmark

This is a performance task for SparkR \*apply API, including spark.lapply, dapply/dapplyCollect, gapply/gapplyCollect.

`define_benchmark.r` generates data in different types and sizes.

`run_benchmark.r` runs tests and save results in results/results.csv and plots in .png files.

## Requirements

- R library: microbenchmark
- databricks/spark

To install microbenchmark, run R script in R shell:
```
install.packages("microbenchmark")
```

Build [Databricks Spark](https://github.com/databricks/spark) locally ([instructions](https://databricks.atlassian.net/wiki/spaces/UN/pages/194805843/0.+Building+and+Running+Spark+Locally)).

Use the path to the root of the above repository as SPARK_HOME, and use it in the shell command below.

## How to run

In shell:

```
sparkr-tests $ ./run_benchmark.sh <your SPARK_HOME> small      # run small test (~10 min)
sparkr-tests $ ./run_benchmark.sh <your SPARK_HOME> medium     # run medium test (~30 min)
sparkr-tests $ ./run_benchmark.sh <your SPARK_HOME> large      # run large test (~90 min)
```

## Synthetic Data


For benchmarking spark.lapply, we generate
```
    lists 
```
1. with different types (7):
```
    Length = 100,
    Type = integer, 
           logical, 
           double, 
           character1, 
           character10, 
           character100, 
           character1k. 
```
2. with different lengths (4):
```
    Type = integer,
    Length = 10,
             100,
             1k,
             10k
```
For benchmarking dapply+rnow/dapplyCollect, we generate
```
    data.frame
```
1. with different types (7):
```
    nrow = 100,
    ncol = 1,
    Type = integer, 
           logical, 
           double, 
           character1, 
           character10, 
           character100, 
           character1k.
```
2. with different lengths (4):
```
    ncol = 1,
    Type = integer,
    nrow = 10,
           100,
           1k,
           10k
```
3. with different ncols (3):
```
    nrow = 100,
    Type = integer,
    ncol = 1,
           10,
           100
```
For benchmarking gapply+rnow/gapplyCollect, we generate
```
    data.frame
```
1. with different number of keys (3):
```
    ncol = 2,
    nrow = 1k,
    Type = <integer, double>,
    nkeys = 10,
            100,
            1000
```
2. with different lengths (4):
```
    ncol = 2,
    Type = <integer, double>,
    nkeys = 10,
    nrow = 10,
           100,
           1k,
           10k
```
3. with different key types (3):
```
    ncol = 2,
    nkeys = 10,
    nrow = 1k,
    Type = <integer, double>
           <character10, double>
           <character100, double>
```