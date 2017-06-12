export RCU_HTM_NR_EXECUTIONS=10
export RCU_HTM_RUNTIME=2
export RCU_HTM_WORKLOADS="100_0_0 80_10_10 0_50_50"
export RCU_HTM_INIT_SIZES="100_100 1000_1K 10000_10K 1000000_1M 10000000_10M"
export RCU_HTM_INIT_SIZES_LABELS="100 1K 10K 1M 10M"
export RCU_HTM_THREADS_CONF="1 2 4 8 16 22 44"
export RCU_HTM_EXECUTABLES="x.avl.bronson x.bst.aravind x.bst.citrus x.avl.int.rcu_sgl x.avl.int.cop x.avl.int.rcu_htm x.rbt.int.rcu_htm"
export RCU_HTM_PLOT_LABELS="lb-avl lf-bst citrus-bst rcu-mrsw-avl cop-avl rcu-htm-avl rcu-htm-rbt"
export RCU_HTM_SERIAL_EXE="x.avl.int.seq"

function rcu_htm_print_env {
	echo "---> RCU_HTM execution environment variables"
	echo "RCU_HTM_NR_EXECUTIONS: $RCU_HTM_NR_EXECUTIONS"
	echo "RCU_HTM_RUNTIME: $RCU_HTM_RUNTIME"
	echo "RCU_HTM_WORKLOADS: $RCU_HTM_WORKLOADS"
	echo "RCU_HTM_INIT_SIZES: $RCU_HTM_INIT_SIZES"
	echo "RCU_HTM_INIT_SIZES_LABELS: $RCU_HTM_INIT_SIZES_LABELS"
	echo "RCU_HTM_THREADS_CONF: $RCU_HTM_THREADS_CONF"
	echo "RCU_HTM_EXECUTABLES: $RCU_HTM_EXECUTABLES"
	echo "RCU_HTM_SERIAL_EXE: $RCU_HTM_SERIAL_EXE"
	echo -e "\n"
}
export -f rcu_htm_print_env
