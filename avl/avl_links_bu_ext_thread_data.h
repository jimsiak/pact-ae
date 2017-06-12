#ifndef _AVL_LINKS_BU_EXT_THREAD_DATA_H_
#define _AVL_LINKS_BU_EXT_THREAD_DATA_H_

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
#	endif
} avl_thread_data_t;

void *avl_thread_data_new(int tid)
{
	avl_thread_data_t *data;

	XMALLOC(data, 1);
	memset(data, 0, sizeof(*data));
	data->tid = tid;
	return data;
}

void avl_thread_data_print(avl_thread_data_t *data)
{
	int i;

	if (data->tid == -1) {
#		ifdef VERBOSE_STATISTICS
		printf("RES_AT_LEVEL:");
		for (i=0; i < MAX_TREE_LEVEL; i++)
			printf(" %8lu", data->restructures_at_level[i]);
		printf("\n");
#		endif
	}
}

void avl_thread_data_add(avl_thread_data_t *d1, 
                         avl_thread_data_t *d2,
                         avl_thread_data_t *dst)
{
	int i;

#	ifdef VERBOSE_STATISTICS
	for (i=0; i < MAX_TREE_LEVEL; i++) {
		dst->restructures_at_level[i] = d1->restructures_at_level[i] +
		                                d2->restructures_at_level[i];
		dst->passed_from_level[i] = d1->passed_from_level[i] +
		                            d2->passed_from_level[i];
	}
#	endif
}

#endif /* _AVL_LINKS_BU_EXT_THREAD_DATA_H_ */
