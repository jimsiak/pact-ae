#!/bin/bash

if [ "x$1" == "x" ]; then
	echo "usage: $0 outputs-dir [nthreads (default 22)]"
	exit 1
fi
if [ ! -d "$1" ]; then
	echo "'$1' does not exist. Please provide an existing folder."
	exit 1;
fi

results_dir=$1
nthreads=22
[ "x$2" != "x" ] && nthreads=$2

./get_averages_normalized.sh $results_dir $nthreads > averages.output
if [ "$?" != "0" ]; then
	echo "Could not find the appropriate values for $nthreads threads in the specified outputs directory."
	rm averages.output
	exit 1
fi

echo "Plotting total averages"
./plotbars_performance_evaluation.py total averages.output
echo "Plotting per tree size averages"
./plotbars_performance_evaluation.py tree_size averages.output
echo "Plotting per workload averages"
./plotbars_performance_evaluation.py workload averages.output

mv performance_evaluation_bars*.png plots
rm averages.output
