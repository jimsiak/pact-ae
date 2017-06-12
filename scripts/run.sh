#!/bin/bash

TIMES=$RCU_HTM_NR_EXECUTIONS
RUNTIME=$RCU_HTM_RUNTIME
WORKLOADS=$RCU_HTM_WORKLOADS
INIT_SIZES=$RCU_HTM_INIT_SIZES
THREADS_CONF=$RCU_HTM_THREADS_CONF
EXECUTABLES=$RCU_HTM_EXECUTABLES
SERIAL_EXE=$RCU_HTM_SERIAL_EXE
if [ "$TIMES" == "" -o "$RUNTIME" == "" -o "$INIT_SIZES" == "" \
	  -o "$WORKLOADS" == "" -o "$THREADS_CONF" == "" -o "$EXECUTABLES" == "" \
	  -o "$SERIAL_EXE" == "" ]; then
	echo "Make sure you have sourced the 'source_me.sh' file."
	exit 1
fi

SEED1=$RANDOM
SEED2=$RANDOM

HOSTNAME=$(hostname)
DIRNAME=results/`date +"%Y_%m_%d-%H_%M-$HOSTNAME"`
mkdir -p $DIRNAME
mkdir -p $DIRNAME/SERIAL
echo "Hostname: $HOSTNAME" > $DIRNAME/INFORMATION
rcu_htm_print_env >> $DIRNAME/INFORMATION

for t in `seq 0 $((TIMES-1))`; do

for INIT_SIZE in $INIT_SIZES; do
	init_size=$(echo $INIT_SIZE | cut -d'_' -f'1')
	init_prefix=$(echo $INIT_SIZE | cut -d'_' -f'2')

for WORKLOAD in $WORKLOADS; do
	lookup_pct=$(echo $WORKLOAD | cut -d'_' -f'1')
	insert_pct=$(echo $WORKLOAD | cut -d'_' -f'2')
	delete_pct=$(echo $WORKLOAD | cut -d'_' -f'3')

for EXECUTABLE in $SERIAL_EXE $EXECUTABLES; do

for thr in $THREADS_CONF; do

	## Run sequential implementation only with 1 thread (used as the baseline
	## when normalizing speedups)
	if [ "$EXECUTABLE" == "$SERIAL_EXE" -a "$thr" != "1" ]; then
		continue
	fi

	echo "$EXECUTABLE $INIT_SIZE $WORKLOAD ($thr threads)"
	FILENAME=${EXECUTABLE}.${init_prefix}_init.${lookup_pct}_${insert_pct}_${delete_pct}.$t.output
	./$EXECUTABLE -s$init_size -m$((2*init_size)) \
	              -l$lookup_pct -i$insert_pct -t$thr \
				  -r$RUNTIME \
				  -e$SEED1 -j$SEED2 \
	              &>> $DIRNAME/$FILENAME

	## Move the outputs of the serial execution in SERIAL dir
	if [ "$EXECUTABLE" == "$SERIAL_EXE" ]; then
		mv $DIRNAME/${SERIAL_EXE}* $DIRNAME/SERIAL
	fi

done # thr
done # EXECUTABLE
done # WORKLOAD
done # INIT_SIZE
done # t

