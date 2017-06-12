#!/bin/bash

plot_script=./plot-nthreads-VS-throughput_averages.py

if [ "x$1" == "x" ]; then
	echo "usage: $0 outputs-dir"
	exit 1
fi
if [ ! -d "$1" ]; then
	echo "'$1' does not exist. Please provide an existing folder."
	exit 1;
fi

WORKLOADS=$RCU_HTM_WORKLOADS
INIT_SIZES=$RCU_HTM_INIT_SIZES_LABELS
if [ "$INIT_SIZES" == "" -o "$WORKLOADS" == "" ]; then
	echo "Make sure you have sourced the 'source_me.sh' file."
	exit 1
fi

for w in $WORKLOADS; do
for i in $INIT_SIZES; do
	echo "Plotting WORKLOAD=$w INIT_SIZE=$i"
	$plot_script $1/*${i}*${w}*
	mv nthreads-VS-throughput.png plots/nthreads-VS-throughput-${i}-${w}.png
done
done
