#ifndef _RBT_LINKS_TD_EXT_THREAD_DATA_H_
#define _RBT_LINKS_TD_EXT_THREAD_DATA_H_

#include <string.h>
#include "alloc.h" /* XMALLOC() */

#ifdef VERBOSE_STATISTICS
#	define MAX_TREE_LEVEL 40 
#endif

typedef struct {
	int tid;
	void *priv;

#	ifdef VERBOSE_STATISTICS
	unsigned long restructures_at_level[MAX_TREE_LEVEL];
	unsigned long passed_from_level[MAX_TREE_LEVEL];
	unsigned long fixups_nr_recolors[MAX_TREE_LEVEL];
#	endif
} td_ext_thread_data_t;

void *td_ext_thread_data_new(int tid)
{
	td_ext_thread_data_t *data;

	XMALLOC(data, 1);
	memset(data, 0, sizeof(*data));
	data->tid = tid;
	return data;
}

void td_ext_thread_data_print(td_ext_thread_data_t *data)
{
	int i;

	if (data->tid == -1) {
#		ifdef VERBOSE_STATISTICS
		printf("RES_AT_LEVEL:");
		for (i=0; i < MAX_TREE_LEVEL; i++) {
//			printf(" %8lu(%lu)", data->restructures_at_level[i],
//			                        data->passed_from_level[i]);
//			printf(" %8lu", data->restructures_at_level[i]);
			printf(" %8lu", data->fixups_nr_recolors[i]);
		}
		printf("\n");
#		endif
	}
}

void td_ext_thread_data_add(td_ext_thread_data_t *d1, 
                            td_ext_thread_data_t *d2,
                            td_ext_thread_data_t *dst)
{
	int i;

#	ifdef VERBOSE_STATISTICS
	for (i=0; i < MAX_TREE_LEVEL; i++) {
		dst->restructures_at_level[i] = d1->restructures_at_level[i] +
		                                d2->restructures_at_level[i];
		dst->passed_from_level[i] = d1->passed_from_level[i] +
		                            d2->passed_from_level[i];
		dst->fixups_nr_recolors[i] = d1->fixups_nr_recolors[i] +
		                             d2->fixups_nr_recolors[i];
	}
#	endif
}
#endif /* _RBT_LINKS_TD_EXT_THREAD_DATA_H_ */
