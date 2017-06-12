#!/bin/bash

EXECUTABLES=$RCU_HTM_EXECUTABLES
INIT_SIZES=$RCU_HTM_INIT_SIZES_LABELS
WORKLOADS=$RCU_HTM_WORKLOADS
if [ "$EXECUTABLES" == "" -o "$INIT_SIZES" == "" -o "$WORKLOADS" == "" ]; then
	echo "Make sure you have sourced the 'source_me.sh' file."
	exit 1
fi
if [ $# -ne 2 ]; then
	echo "usage: $0 results_dir nthreads"
	exit 1
fi

results_dir=$1
nthreads=$2

echo "#### NTHREADS $nthreads"

for s in $INIT_SIZES; do
for w in $WORKLOADS; do
	serial_average_throughput=$(./get_average_nthreads.py $results_dir/SERIAL/x.avl.int.seq.${s}_init.${w}* | grep "nthreads 1:" | cut -d' ' -f3)
for x in $EXECUTABLES; do
	average_throughput=$(./get_average_nthreads.py $results_dir/$x.${s}_init.${w}* | grep "nthreads $nthreads:" | cut -d' ' -f3)

	if [ "$serial_average_throughput" == "" -o "$average_throughput" == "" ]; then
		echo "ERROR: could not read serial of parallel throughput." 1>&2
		exit 1
	fi

	normalized=$(echo "$average_throughput/$serial_average_throughput" | bc -l)
	echo $x $s $w $normalized
done
done
done
