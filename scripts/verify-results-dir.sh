#!/bin/bash

if [ "x$1" == "x" ]; then
	echo "usage: $0 outputs-dir"
	exit 1
fi
if [ ! -d "$1" ]; then
	echo "'$1' does not exist. Please provide an existing folder."
	exit 1;
fi

NR_EXECUTIONS=$RCU_HTM_NR_EXECUTIONS
WORKLOADS=$RCU_HTM_WORKLOADS
INIT_SIZES=$RCU_HTM_INIT_SIZES_LABELS
THREADS_CONF=$RCU_HTM_THREADS_CONF
EXECUTABLES=$RCU_HTM_EXECUTABLES
SERIAL_EXE=$RCU_HTM_SERIAL_EXE
if [ "$NR_EXECUTIONS" == "" -o "$INIT_SIZES" == "" -o "$WORKLOADS" == "" \
     -o "$THREADS_CONF" == "" -o "$EXECUTABLES" == "" -o "$SERIAL_EXE" == "" ]; then
	echo "Make sure you have sourced the 'source_me.sh' file."
	exit 1
fi

outputs_dir=$1
THREADS_CONF_LEN=$(echo "$THREADS_CONF" | wc -w)
ERROR_FOUND=0

for n in `seq 0 $((NR_EXECUTIONS-1))`; do
for e in $EXECUTABLES; do
for w in $RCU_HTM_WORKLOADS; do
for i in $INIT_SIZES; do

	filename="$outputs_dir/${e}.${i}_init.${w}.${n}.output"
	nr_throughput_lines=$(grep "Throughput(Ops/usec):" $filename | wc -l)
	if [ "$nr_throughput_lines" != "$THREADS_CONF_LEN" ]; then
		echo "ERROR: filename $filename has $nr_throughput_lines throughput values instead of $THREADS_CONF_LEN"
		ERROR_FOUND=1
	fi

done
done
done
done

exit $ERROR_FOUND
