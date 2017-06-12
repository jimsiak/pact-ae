#ifndef _RBT_LINKS_TD_EXT_FG_TSX_THREAD_DATA_H_
#define _RBT_LINKS_TD_EXT_FG_TSX_THREAD_DATA_H_

#include <string.h>
#include "alloc.h" /* XMALLOC() */

#if !defined(TX_STATS_ARRAY_NR_TRANS)
#	error "You should define `TX_STATS_ARRAY_NR_TRANS`"
#endif

#ifdef VERBOSE_STATISTICS
#	define MAX_TREE_LEVEL 40 
#endif

typedef struct {
	int tid;

#	ifdef VERBOSE_STATISTICS
	unsigned long restructures_at_level[MAX_TREE_LEVEL];
	unsigned long windows_with_size[MAX_TREE_LEVEL];
	unsigned long aborts_per_level[MAX_TREE_LEVEL];
	unsigned long tx_con_aborts_per_level[MAX_TREE_LEVEL];
	unsigned long non_tx_con_aborts_per_level[MAX_TREE_LEVEL];
	unsigned long starts_per_level[MAX_TREE_LEVEL];
	unsigned long lacqs_per_level[MAX_TREE_LEVEL];
#	endif

	unsigned long long insert_cases[3];

	unsigned long long tx_starts,
	                   tx_aborts,
	                   tx_aborts_version_error,
	                   tx_aborts_footprint_overflow,
	                   tx_aborts_transaction_conflict,
	                   tx_aborts_non_transaction_conflict,
	                   tx_aborts_rest,
	                   tx_lacqs;

	/* First dimension:  0 -> lookup, 1 -> insert, 2 -> delete.
	   Second dimension: 0 -> First transaction, 1 -> second.
	   Third dimension:  0 -> tx_starts, 1 -> tx_aborts,
	                     2 -> transaction_conflict, 
	                     3 -> non_transaction_conflict,
	                     4 -> footprint_overflow, 5 -> aborts_version_error,
	                     6 -> aborts_rest
	                     7 -> retries_from_scratch
	 */
	unsigned long long tx_stats[3][TX_STATS_ARRAY_NR_TRANS][8];
} htm_fg_tdata_t;

static htm_fg_tdata_t *htm_fg_tdata_new(int tid)
{
	htm_fg_tdata_t *ret;

	XMALLOC(ret, 1);
	memset(ret, 0, sizeof(*ret));

	ret->tid = tid;
	return ret;
}

static void htm_fg_tdata_print(htm_fg_tdata_t *data)
{
//	printf("RBT_THREAD_DATA: %3d %12llu %12llu %12llu ( %12llu %12llu ) "
//	       "INSERT ( %12llu %12llu %12llu )\n", data->tid,
//	       data->tx_starts, data->tx_starts - data->tx_aborts, data->tx_aborts,
//	       data->tx_aborts_footprint_overflow, data->tx_aborts_version_error,
//		   data->insert_cases[0], data->insert_cases[1], data->insert_cases[2]);

	if (data->tid == -1) {
		printf("RBT_THREAD_DATA: %3d %12llu %12llu %12llu "
		       "( %12llu %12llu %12llu %12llu %12llu ) %12llu "
		       "INSERT ( %12llu %12llu %12llu )\n", data->tid,
		       data->tx_starts, 
		       data->tx_starts - data->tx_aborts, data->tx_aborts,
		       data->tx_aborts_transaction_conflict,
		       data->tx_aborts_non_transaction_conflict,
		       data->tx_aborts_footprint_overflow, 
		       data->tx_aborts_version_error, 
		       data->tx_aborts_rest,
		       data->tx_lacqs,
			   data->insert_cases[0], data->insert_cases[1],
		       data->insert_cases[2]);

		char titles[3][20] = {"Lookup", "Insert", "Delete"};
		int i, j, k;
		for (i=0; i < 3; i++) {
			printf("%s:\n", titles[i]);
			for (j=0; j < TX_STATS_ARRAY_NR_TRANS; j++) {
				printf("  ");
				for (k=0; k < 7; k++) {
					printf(" %12llu", data->tx_stats[i][j][k]);
				}
				printf(" (");
				for (k=7; k < 8; k++) {
					printf(" %12llu", data->tx_stats[i][j][k]);
				}
				printf(" )\n");
			}
		}
#		ifdef VERBOSE_STATISTICS
		printf("RES_AT_LEVEL:");
		for (i=0; i < MAX_TREE_LEVEL; i++)
			printf(" %8lu", data->restructures_at_level[i]);
		printf("\n");
//		printf("WINDOWS_WITH_SIZE:");
//		for (i=0; i < MAX_TREE_LEVEL; i++)
//			printf(" %8lu", data->windows_with_size[i]);
//		printf("\n");
		printf("LACQS_PER_LEVEL:");
		for (i=0; i < MAX_TREE_LEVEL; i++)
			printf(" %8lu", data->lacqs_per_level[i]);
		printf("\n");
		printf("ABORTS_PER_LEVEL:");
		for (i=0; i < MAX_TREE_LEVEL; i++)
			printf(" %8lu", data->aborts_per_level[i]);
		printf("\n");
		printf("ABORTS_PER_LEVEL:");
		for (i=0; i < MAX_TREE_LEVEL; i++)
			printf(" %8lu", data->tx_con_aborts_per_level[i]);
		printf("\n");
		printf("ABORTS_PER_LEVEL:");
		for (i=0; i < MAX_TREE_LEVEL; i++)
			printf(" %8lu", data->non_tx_con_aborts_per_level[i]);
		printf("\n");
#		endif
	}
}

static void htm_fg_tdata_add(htm_fg_tdata_t *d1, htm_fg_tdata_t *d2,
                             htm_fg_tdata_t *dst)
{
	int i, j, k;

	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->tx_aborts_transaction_conflict = d1->tx_aborts_transaction_conflict +
	                                      d2->tx_aborts_transaction_conflict;
	dst->tx_aborts_non_transaction_conflict = 
	                                   d1->tx_aborts_non_transaction_conflict +
	                                   d2->tx_aborts_non_transaction_conflict;
	dst->tx_aborts_version_error = d1->tx_aborts_version_error + 
	                               d2->tx_aborts_version_error;
	dst->tx_aborts_footprint_overflow = d1->tx_aborts_footprint_overflow + 
	                               d2->tx_aborts_footprint_overflow;
	dst->tx_aborts_rest = d1->tx_aborts_rest + d2->tx_aborts_rest;
	dst->tx_lacqs = d1->tx_lacqs + d2->tx_lacqs;

	for (i=0; i < 3; i++)
		dst->insert_cases[i] = d1->insert_cases[i] + d2->insert_cases[i];

	for (i=0; i < 3; i++)
		for (j=0; j < TX_STATS_ARRAY_NR_TRANS; j++)
			for (k=0; k < 8; k++)
				dst->tx_stats[i][j][k] = d1->tx_stats[i][j][k] + 
				                         d2->tx_stats[i][j][k];

#	ifdef VERBOSE_STATISTICS
	for (i=0; i < MAX_TREE_LEVEL; i++) {
		dst->restructures_at_level[i] = d1->restructures_at_level[i] +
		                                d2->restructures_at_level[i];
		dst->windows_with_size[i] = d1->windows_with_size[i] +
		                            d2->windows_with_size[i];
		dst->aborts_per_level[i] = d1->aborts_per_level[i] +
		                           d2->aborts_per_level[i];
		dst->tx_con_aborts_per_level[i] = d1->tx_con_aborts_per_level[i] +
		                                  d2->tx_con_aborts_per_level[i];
		dst->non_tx_con_aborts_per_level[i] = d1->non_tx_con_aborts_per_level[i] +
		                                  d2->non_tx_con_aborts_per_level[i];
		dst->lacqs_per_level[i] = d1->lacqs_per_level[i] +
		                          d2->lacqs_per_level[i];
		dst->starts_per_level[i] = d1->starts_per_level[i] +
		                           d2->starts_per_level[i];
	}
#	endif

}
#endif  /* _RBT_LINKS_TD_EXT_FG_TSX_THREAD_DATA_H_ */
