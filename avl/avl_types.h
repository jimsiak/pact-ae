#ifndef _AVL_TYPES_H_
#define _AVL_TYPES_H_

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h" /* XMALLOC() */

typedef struct avl_node {
	int height;
	int key;
	void *value;
	struct avl_node *link[2];

#	ifdef USE_VERSIONING
	unsigned long long version; /* A version number, starts from 1. */
#	endif

	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) - 
	             2 * sizeof(struct avl_node *)
#	ifdef USE_VERSIONING
	             - sizeof(unsigned long long)
#	endif
	             ];
} __attribute__((aligned(CACHE_LINE_SIZE))) avl_node_t;

typedef struct {
	avl_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(USE_VERSIONING)
	pthread_spinlock_t global_lock;
#	endif

#	ifdef USE_VERSIONING
	unsigned long long version;
#	endif
} avl_t;

static avl_node_t *avl_node_new(int key, void *value)
{
	avl_node_t *ret;

	XMALLOC(ret, 1);
	ret->height = 0;
	ret->key = key;
	ret->value = value;
	ret->link[0] = NULL;
	ret->link[1] = NULL;

#	ifdef USE_VERSIONING
	ret->version = 1;
#	endif

	return ret;
}

static inline avl_t *_avl_new_helper()
{
	avl_t *ret;

	XMALLOC(ret, 1);
	ret->root = NULL;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(USE_VERSIONING)
	pthread_spin_init(&ret->global_lock, PTHREAD_PROCESS_SHARED);
#	endif

#	ifdef USE_VERSIONING
	ret->version = 1;
#	endif

	return ret;
}

#endif /* _AVL_TYPES_H_ */
