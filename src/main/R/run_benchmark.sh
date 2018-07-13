#!/bin/bash
# Usage: ./run_benchmark.sh /Users/liang/spark small
if [[ ($# -ne 1 && $# -ne 2) ]]; then
  echo "Usage: $0 <your SPARK_HOME> [small/medium/large] (default: small)" >&2
  exit 1
fi
if ! [ -e "$1" ]; then
  echo "$1 not found" >&2
  exit 1
fi
if ! [ -d "$1" ]; then
  echo "$1 not a directory" >&2
  exit 1
fi
if [[ ( $# -eq 2 && ($2 -ne "small" || $2 -ne "medium" || $2 -ne "large" ) ) ]]; then
  echo "The second argument should be 'small' or 'medium' or 'large'" >&2
  exit 1
fi

mkdir -p results

export SPARK_HOME=$1
export R_LIBS_USER=$SPARK_HOME/R/lib

if [[ $# -eq 1 ]]; then
	Rscript run_benchmark.r small
else
	Rscript run_benchmark.r $2
fi
