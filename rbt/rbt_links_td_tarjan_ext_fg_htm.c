#include <pthread.h> //> pthread_spinlock_t
#include <htmintrin.h> /* power8 tm gcc intrinsics. */
#include <assert.h>

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h" /* XMALLOC() */

#define TX_STATS_ARRAY_NR_TRANS 2
#include "rbt_links_td_ext_fg_htm_thread_data.h"

#if !defined(TX_NUM_RETRIES)
#	define TX_NUM_RETRIES 20
#endif

#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )
#define IS_BLACK(node) ( !(node) || !(node)->is_red )
#define IS_RED(node) ( !IS_BLACK(node) )
#define GET_VERSION(node) ( (!(node)) ? 0 : (node)->version )
//#define INC_VERSION(node) (node)->version++
#define INC_VERSION(node) do { if ((node))  { (node)->version++; } } while(0)

#define IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN 0xfe
#define IMPLICIT_ABORT_VERSION_ERROR 0xee

#ifdef USE_CPU_LOCK
#	define NR_CPUS 20
struct {
	pthread_spinlock_t spinlock;
	int owner; /* A per cpu lock. Its value is the pid of the owner. */
	char padding[CACHE_LINE_SIZE - sizeof(int) - sizeof(pthread_spinlock_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) cpu_locks[NR_CPUS];
#endif

#ifndef ACCESS_PATH_MAX_DEPTH
#	define ACCESS_PATH_MAX_DEPTH 0
#endif

#define ABORT_HANDLER(tdata, op, tx) \
	do { \
		tdata->tx_aborts++; \
		tdata->tx_stats[op][tx][1]++; \
		texasru_t texasru = __builtin_get_texasru(); \
		if (_TEXASRU_ABORT(texasru) &&  \
		    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) { \
			tdata->tx_stats[op][tx][5]++; \
			tdata->tx_aborts_version_error++; \
			goto try_from_scratch; \
		} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) { \
			tdata->tx_stats[op][tx][4]++; \
			tdata->tx_aborts_footprint_overflow++; \
		} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) { \
			tdata->tx_stats[op][tx][2]++; \
			tdata->tx_aborts_transaction_conflict++; \
		} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) { \
			tdata->tx_stats[op][tx][3]++; \
			tdata->tx_aborts_non_transaction_conflict++; \
		} else { \
			tdata->tx_stats[op][tx][6]++; \
			tdata->tx_aborts_rest++; \
		} \
	} while (0)

typedef struct rbt_node {
	int is_red;
	int key;
	void *value;
	struct rbt_node *link[2];

	unsigned long long int version; /* A version numbers, starts from 1. */

	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) - 
	             2 * sizeof(struct rbt_node *) - sizeof(unsigned long long)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;

typedef struct {
	rbt_node_t *root;
	char padding0[CACHE_LINE_SIZE - sizeof(rbt_node_t *)];

	unsigned long long int version;
	char padding1[CACHE_LINE_SIZE - sizeof(unsigned long long)];

	pthread_spinlock_t spinlock;
	char padding2[CACHE_LINE_SIZE - sizeof(pthread_spinlock_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_t;

static rbt_node_t *rbt_node_new(int key, void *value)
{
	rbt_node_t *ret;

	XMALLOC(ret, 1);
	ret->is_red = 1;
	ret->key = key;
	ret->value = value;
	ret->link[0] = NULL;
	ret->link[1] = NULL;
	ret->version = 1;

	return ret;
}

static inline rbt_t *_rbt_new_helper()
{
	rbt_t *ret;

	XMALLOC(ret, 1);
	ret->root = NULL;
	ret->version = 1;

	pthread_spin_init(&ret->spinlock, PTHREAD_PROCESS_SHARED);

#	ifdef USE_CPU_LOCK
	int i;
	for (i=0; i < NR_CPUS; i++) {
		pthread_spin_init(&cpu_locks[i].spinlock, PTHREAD_PROCESS_SHARED);
		cpu_locks[i].owner= -1;
	}
#	endif

	return ret;
}

static rbt_node_t *rbt_rotate_single(rbt_node_t *root, int dir)
{
	rbt_node_t *save = root->link[!dir];

	root->link[!dir] = save->link[dir];
	save->link[dir] = root;

	return save;
}

static rbt_node_t *rbt_rotate_double(rbt_node_t *root, int dir)
{
	root->link[!dir] = rbt_rotate_single(root->link[!dir], !dir);
	return rbt_rotate_single(root, dir);
}

static int _rbt_lookup_helper_serial(rbt_t *rbt, int key)
{
	int dir;
	rbt_node_t *curr;

	if (!rbt->root)
		return 0;

	curr = rbt->root;
	while (!IS_EXTERNAL_NODE(curr)) {
		dir = curr->key < key;
		curr = curr->link[dir];
	}

	int found = (curr->key == key);
	return found;
}

static int _rbt_lookup_helper(rbt_t *rbt, void *thread_data, int key,
                              htm_fg_tdata_t *tdata)
{
	int dir, found;
	rbt_node_t *curr;
	unsigned long long window_versions[1]; /* curr version */
	int retries, tx1_retries, tx2_retries;

#	ifdef USE_CPU_LOCK
	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;
#	endif

	retries = -1;
try_from_scratch:

	retries++;
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner == tid) {
			cpu_locks[my_cpu_lock].owner = -1;
			pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
		}
#		endif
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_lookup_helper_serial(rbt, key);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	tx1_retries = -1;
TX1:
	tx1_retries++;
	if (tx1_retries >= TX_NUM_RETRIES)
		goto try_from_scratch;

	/* Avoid Lemming effect. */
#	ifdef USE_CPU_LOCK
	while (cpu_locks[my_cpu_lock].owner > 0 &&
	       cpu_locks[my_cpu_lock].owner != tid &&
	       cpu_locks[my_cpu_lock].spinlock == 1)
		;
#	endif
	while (rbt->spinlock == 1)
		;

	/* First transaction at the root. */
	tdata->tx_starts++;
	tdata->tx_stats[0][0][0]++;
	if (__builtin_tbegin(0)) {
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner > 0 &&
		    cpu_locks[my_cpu_lock].owner != tid &&
		    cpu_locks[my_cpu_lock].spinlock == 1)
			__builtin_tabort(0x77);
#		endif
		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

		/* Empty tree. */
		if (!rbt->root) {
			__builtin_tend(0);
			return 0;
		}

		curr = rbt->root;
		window_versions[0] = GET_VERSION(curr);
		__builtin_tend(0);
	} else {
		ABORT_HANDLER(tdata, 0, 0);
		goto TX1;
	}

	/* Walk down the tree. */
	while (1) {
		tx2_retries = -1;
TX2:
		tx2_retries++;
		if (tx2_retries >= TX_NUM_RETRIES)
			goto try_from_scratch;

		/* Avoid Lemming effect. */
#		ifdef USE_CPU_LOCK
		while (cpu_locks[my_cpu_lock].owner > 0 && 
		       cpu_locks[my_cpu_lock].owner != tid &&
		       cpu_locks[my_cpu_lock].spinlock == 1)
			;
#		endif
		while (rbt->spinlock == 1)
			;

		tdata->tx_starts++;
		tdata->tx_stats[0][1][0]++;
		if (__builtin_tbegin(0)) {
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner > 0 && 
			    cpu_locks[my_cpu_lock].owner != tid &&
			    cpu_locks[my_cpu_lock].spinlock == 1)
				__builtin_tabort(0x77);
#			endif
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			if (window_versions[0] != GET_VERSION(curr))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			/* External node reached. */
			if (IS_EXTERNAL_NODE(curr)) {
				found = (curr->key == key);
				__builtin_tend(0);
				return found;
			}

			/* Move to the next node. */
			dir = curr->key < key;
			curr = curr->link[dir];

			window_versions[0] = GET_VERSION(curr);

			__builtin_tend(0);
		} else {
			ABORT_HANDLER(tdata, 0, 1);
			goto TX2;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static int _insert_fix_violation(rbt_t *rbt, int key,
                                  rbt_node_t **node_stack, int top,
                                  htm_fg_tdata_t *tdata, int wroot_level,
                                  rbt_node_t *root_parent)
{
	rbt_node_t *curr, *parent, *sibling, *gparent;
	int dir_from_parent, dir_from_gparent;

	top--; /* Ignore the red child. */

	while (top > 1) {
		curr = node_stack[top--];

		if (IS_BLACK(curr))
			return 0;

		parent = node_stack[top];
		dir_from_parent = parent->key < key;
		sibling = parent->link[!dir_from_parent];
		if (IS_RED(sibling)) {
			parent->is_red = 1;
			curr->is_red = 0;
			sibling->is_red = 0;
			top--;

			INC_VERSION(node_stack[top-1]);
			INC_VERSION(parent);
			INC_VERSION(curr);
			INC_VERSION(sibling);
		} else { /* IS_BLACK(sibling) */
			gparent = node_stack[top-1];
			dir_from_gparent = gparent->key < key;
			INC_VERSION(gparent);
			INC_VERSION(parent);
			INC_VERSION(curr);
			INC_VERSION(curr->link[0]);
			INC_VERSION(curr->link[1]);
			if (IS_RED(curr->link[dir_from_parent])) {
				parent->is_red = 1;
				curr->is_red = 0;
				gparent->link[dir_from_gparent] = 
				     rbt_rotate_single(parent, !dir_from_parent);
				if (gparent == root_parent) {
//				if (parent == rbt->root) {
//				if (wroot_level == 0) {
					rbt->root = gparent->link[dir_from_gparent];
					rbt->version++;
				}
				return 1;
			} else {
				parent->is_red = 1;
				curr->link[!dir_from_parent]->is_red = 0;
				gparent->link[dir_from_gparent] = 
				     rbt_rotate_double(parent, !dir_from_parent);
				if (gparent == root_parent) {
//				if (parent == rbt->root) {
//				if (wroot_level == 0) {
					rbt->root = gparent->link[dir_from_gparent];
					rbt->version++;
				}
				return 2;
			}
		}
	}

	return 0;
}

static int replace_external_node(rbt_node_t *parent, rbt_node_t *external,
                                 rbt_node_t *new[2])
{
	/* Key already there. */
	if (external->key == new[0]->key)
		return 0;

	INC_VERSION(parent);
	INC_VERSION(external);

	external->link[0] = new[0];
	external->link[1] = new[1];
	external->is_red = 1;
	external->link[0]->is_red = 0;
	external->link[1]->is_red = 0;

	if (external->key > new[0]->key) {
		external->link[1]->key = external->key;
		external->key = external->link[0]->key;
	} else {
		external->link[0]->key = external->key;
	}
	return 1;
}

static inline int _rbt_insert_helper_serial(rbt_t *rbt, rbt_node_t *node[2],
                                            htm_fg_tdata_t *tdata)
{
	rbt_node_t *wroot, *wroot_parent;
	rbt_node_t *curr, *left, *right, *next;
	rbt_node_t head = {0}; /* False tree root. */
	rbt_node_t *node_stack[40];
	int dir, rr_sequence, top;
	int level = 0; /* The level of window root. */

	if (!rbt->root) {
		INC_VERSION(rbt->root);
		rbt->version++;
		rbt->root = node[0];
		rbt->root->is_red = 0;
		return 1;
	}

	if (IS_EXTERNAL_NODE(rbt->root)) {
		INC_VERSION(rbt->root);
		rbt->version++;
		return replace_external_node(NULL, rbt->root, node);
	}

	if (IS_RED(rbt->root)) {
		INC_VERSION(rbt->root);
		rbt->root->is_red = 0;
	} else if (IS_RED(rbt->root->link[0]) && IS_RED(rbt->root->link[1])) {
		rbt->root->link[0]->is_red = 0;
		rbt->root->link[1]->is_red = 0;
		INC_VERSION(rbt->root);
		INC_VERSION(rbt->root->link[0]);
		INC_VERSION(rbt->root->link[1]);
	}

	head.link[1] = rbt->root;
	wroot_parent = &head;
	wroot = rbt->root;

	while (1) {
		/* Window root should never be an external node. */
		assert(!IS_EXTERNAL_NODE(wroot));

		top = -1;
		node_stack[++top] = wroot_parent;
		node_stack[++top] = wroot;

		dir = wroot->key < node[0]->key;
		curr = wroot->link[dir];

		rr_sequence = 0;
		while (rr_sequence < ACCESS_PATH_MAX_DEPTH) {
			if (!IS_EXTERNAL_NODE(curr) && IS_BLACK(curr) &&
			    IS_RED(curr->link[0]) && IS_RED(curr->link[1])) {

				node_stack[++top] = curr;

				/* Bypass the red-red children. */
				dir = curr->key < node[0]->key;
				curr = curr->link[dir];
				node_stack[++top] = curr;

				dir = curr->key < node[0]->key;
				curr = curr->link[dir];

				rr_sequence++;
			} else if (IS_RED(curr)) {
				node_stack[++top] = curr;
				dir = curr->key < node[0]->key;
				curr = curr->link[dir];
			} else {
				break;
			}
		}

		node_stack[++top] = curr;

		if (IS_EXTERNAL_NODE(curr)) {
			if (replace_external_node(node_stack[top-1], curr, node)) {
				_insert_fix_violation(rbt, node[0]->key, node_stack, top,
				                      tdata, level, &head);
				return 1;
			}
			return 0;
		}

		left = curr->link[0];
		right = curr->link[1];

		if (IS_BLACK(curr)) {
			if (IS_BLACK(left) || IS_BLACK(right)) {
				wroot_parent = node_stack[top-1];
				wroot = curr;
				level += top - 1;
				continue;
			} else {
				INC_VERSION(node_stack[top-1]);
				INC_VERSION(curr);
				INC_VERSION(left);
				INC_VERSION(right);
				curr->is_red = 1;
				left->is_red = 0;
				right->is_red = 0;
				char ret = _insert_fix_violation(rbt, node[0]->key,
				                                 node_stack, top, tdata, level,
				                                 &head);

				dir = curr->key < node[0]->key;
				wroot = curr->link[dir];
				wroot_parent = curr;
				level += top - ret;
				continue;
			}
		}

		/* (IS_RED(curr)) */
		dir = curr->key < node[0]->key;
		next = curr->link[dir];

		node_stack[++top] = next;
		if (IS_EXTERNAL_NODE(next)) {
			if (replace_external_node(curr, next, node)) {
				_insert_fix_violation(rbt, node[0]->key, node_stack, top,
				                      tdata, level, &head);
				return 1;
			}
			return 0;
		}

		if (IS_BLACK(next->link[0]) || IS_BLACK(next->link[1])) {
			wroot_parent = curr;
			wroot = next;
			level += top - 1;
			continue;
		} else {
			INC_VERSION(curr);
			INC_VERSION(next);
			INC_VERSION(next->link[0]);
			INC_VERSION(next->link[1]);

			next->is_red = 1;
			next->link[0]->is_red = 0;
			next->link[1]->is_red = 0;
			char ret = _insert_fix_violation(rbt, node[0]->key, node_stack, top,
			                      tdata, level, &head);

			dir = next->key < node[0]->key;
			wroot_parent = next;
			wroot = next->link[dir];
			level += top - ret;
			continue;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static inline int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *node[2],
                                     htm_fg_tdata_t *tdata)
{
	int retries, tx1_retries, tx2_retries;
	unsigned long long window_versions[5];
	rbt_node_t *wroot, *wroot_parent;
	rbt_node_t *curr, *left, *right, *next;
	rbt_node_t head = {0}; /* False tree root. */
	rbt_node_t *node_stack[40];
	int dir, rr_sequence, top;
	int level = 0; /* The level of window root. */

#	ifdef USE_CPU_LOCK
	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;
#	endif

	retries = -1;
try_from_scratch:
	level = 0;

	retries++;
	/* Grab the global lock and serialize. */
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner == tid) {
			cpu_locks[my_cpu_lock].owner = -1;
			pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
		}
#		endif
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_insert_helper_serial(rbt, node, tdata);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	tx1_retries = -1;
TX1:
	tx1_retries++;
	if (tx1_retries >= TX_NUM_RETRIES) {
#		ifdef VERBOSE_STATISTICS
		tdata->lacqs_per_level[level]++;
#		endif
		goto try_from_scratch;
	}

#	ifdef USE_CPU_LOCK
	while (cpu_locks[my_cpu_lock].owner > 0 &&
	       cpu_locks[my_cpu_lock].owner != tid &&
	       cpu_locks[my_cpu_lock].spinlock == 1)
		;
#	endif
	while (rbt->spinlock == 1)
		;

	head.is_red = 0;
	head.version = 1;

	tdata->tx_stats[1][0][0]++;
	tdata->tx_starts++;
	if (__builtin_tbegin(0)) {
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner > 0 &&
		    cpu_locks[my_cpu_lock].owner != tid &&
		    cpu_locks[my_cpu_lock].spinlock == 1)
			__builtin_tabort(0x77);
#		endif
		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

		/* Empty tree. */
		if (!rbt->root) {
			rbt->root = node[0];
			rbt->root->is_red = 0;
			INC_VERSION(rbt->root);
			rbt->version++;
			__builtin_tend(0);
			return 1;
		}

		if (IS_EXTERNAL_NODE(rbt->root)) {
			INC_VERSION(rbt->root);
			rbt->version++;
			int ret = replace_external_node(NULL, rbt->root, node);
			__builtin_tend(0);
			return ret;
		}
	
		if (IS_RED(rbt->root)) {
			rbt->root->is_red = 0;
			INC_VERSION(rbt->root);
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[0]++;
#			endif
		} else if (IS_RED(rbt->root->link[0]) && IS_RED(rbt->root->link[1])) {
			rbt->root->link[0]->is_red = 0;
			rbt->root->link[1]->is_red = 0;
			INC_VERSION(rbt->root);
			INC_VERSION(rbt->root->link[0]);
			INC_VERSION(rbt->root->link[1]);
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[0]++;
#			endif
		}
	
		head.link[1] = rbt->root;
		wroot_parent = &head;
		wroot = rbt->root;
		window_versions[0] = GET_VERSION(wroot_parent);
		window_versions[1] = GET_VERSION(wroot);
		window_versions[2] = GET_VERSION(wroot->link[0]);
		window_versions[3] = GET_VERSION(wroot->link[1]);
		window_versions[4] = rbt->version;
		__builtin_tend(0);
	} else {
		/* Abort. */
#		ifdef VERBOSE_STATISTICS
		texasru_t texasru1 = __builtin_get_texasru();
		if (_TEXASRU_TRANSACTION_CONFLICT(texasru1))
			tdata->tx_con_aborts_per_level[level]++;
		else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru1))
			tdata->non_tx_con_aborts_per_level[level]++;
//		if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru1)) {
//		if (_TEXASRU_ABORT(texasru1) && 
//		    _TEXASRU_FAILURE_CODE(texasru1) == IMPLICIT_ABORT_VERSION_ERROR)
			tdata->aborts_per_level[level]++;
//		}
#		endif

		ABORT_HANDLER(tdata, 1, 0);
		goto TX1;
	}

	/* Window transactions. */
	while (1) {
		tx2_retries = -1;
TX2:
		tx2_retries++;
		if (tx2_retries >= TX_NUM_RETRIES) {
#			ifdef VERBOSE_STATISTICS
			tdata->lacqs_per_level[level]++;
#			endif
			goto try_from_scratch;
		}
#		ifdef USE_CPU_LOCK
		if (tx2_retries >= TX_NUM_RETRIES / 2 &&
		    cpu_locks[my_cpu_lock].owner != tid) {
			pthread_spin_lock(&cpu_locks[my_cpu_lock].spinlock);
			cpu_locks[my_cpu_lock].owner = tid;
		}
#		endif

		/* Avoid lemming effect. */
#		ifdef USE_CPU_LOCK
		while (cpu_locks[my_cpu_lock].owner > 0 && 
		       cpu_locks[my_cpu_lock].owner != tid &&
		       cpu_locks[my_cpu_lock].spinlock == 1)
			;
#		endif
		while (rbt->spinlock == 1)
			;

#		ifdef VERBOSE_STATISTICS
		tdata->starts_per_level[level]++;
#		endif
		tdata->tx_stats[1][1][0]++;
		tdata->tx_starts++;
		if (__builtin_tbegin(0)) {
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner > 0 &&
			    cpu_locks[my_cpu_lock].owner != tid &&
			    cpu_locks[my_cpu_lock].spinlock == 1)
				__builtin_tabort(0x77);
#			endif
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			if (window_versions[0] != GET_VERSION(wroot_parent) ||
			    window_versions[1] != GET_VERSION(wroot) ||
                window_versions[2] != GET_VERSION(wroot->link[0]) ||
			    window_versions[3] != GET_VERSION(wroot->link[1]))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			/* Window root should never be an external node. */
			assert(!IS_EXTERNAL_NODE(wroot));
	
			top = -1;
			node_stack[++top] = wroot_parent;
			node_stack[++top] = wroot;
	
			curr = wroot;
			rr_sequence = 0;
	
			while (1) {
				dir = curr->key < node[0]->key;
				curr = curr->link[dir];
				node_stack[++top] = curr;
	
				if (IS_EXTERNAL_NODE(curr)) {
					if (replace_external_node(node_stack[top-1], curr, node)) {
						_insert_fix_violation(rbt, node[0]->key, node_stack, top,
						                      tdata, level, &head);
						__builtin_tend(0);
#						ifdef USE_CPU_LOCK
						if (cpu_locks[my_cpu_lock].owner == tid) {
							cpu_locks[my_cpu_lock].owner = -1;
							pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
						}
#						endif
						return 1;
					}
					__builtin_tend(0);
#					ifdef USE_CPU_LOCK
					if (cpu_locks[my_cpu_lock].owner == tid) {
						cpu_locks[my_cpu_lock].owner = -1;
						pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
					}
#					endif
					return 0;
				}
	
				if (IS_BLACK(curr) &&
				    (IS_BLACK(curr->link[0]) || IS_BLACK(curr->link[1])))
				{
					wroot_parent = node_stack[top-1];
					wroot = curr;
					level += top - 1;
					break;
				}
	
				if (IS_RED(curr->link[0]) && IS_RED(curr->link[1])) {
					rr_sequence++;
					if (rr_sequence >= ACCESS_PATH_MAX_DEPTH) {
						INC_VERSION(curr);
						INC_VERSION(curr->link[0]);
						INC_VERSION(curr->link[1]);
						curr->is_red = 1;
						curr->link[0]->is_red = 0;
						curr->link[1]->is_red = 0;
						char ret = _insert_fix_violation(rbt, node[0]->key,
						                                 node_stack, top,
						                                 tdata, level, &head);
			
						dir = curr->key < node[0]->key;
						wroot_parent = curr;
						wroot = curr->link[dir];
						level += top - ret;
						break;
					}
				}
			}

			/* Get the version of the next window. */
			window_versions[0] = GET_VERSION(wroot_parent);
			window_versions[1] = GET_VERSION(wroot);
			window_versions[2] = GET_VERSION(wroot->link[0]);
			window_versions[3] = GET_VERSION(wroot->link[1]);
			__builtin_tend(0);
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner == tid) {
				cpu_locks[my_cpu_lock].owner = -1;
				pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
			}
#			endif
		} else {
			/* Abort. */
#			ifdef VERBOSE_STATISTICS
			texasru_t texasru1 = __builtin_get_texasru();
			if (_TEXASRU_TRANSACTION_CONFLICT(texasru1))
				tdata->tx_con_aborts_per_level[level]++;
			else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru1))
				tdata->non_tx_con_aborts_per_level[level]++;
//			if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru1)) {
//			if (_TEXASRU_TRANSACTION_CONFLICT(texasru1)) {
//			if (_TEXASRU_ABORT(texasru1) && 
//			    _TEXASRU_FAILURE_CODE(texasru1) == IMPLICIT_ABORT_VERSION_ERROR)
				tdata->aborts_per_level[level]++;
//			}
#			endif
			ABORT_HANDLER(tdata, 1, 1);
			goto TX2;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}

#define DEL_GET_WIN_VERSION(wroot_parent, wroot, versions) \
	do { \
		versions[0] = GET_VERSION(wroot_parent); \
		versions[1] = GET_VERSION(wroot); \
		versions[2] = GET_VERSION(wroot->link[0]); \
		versions[3] = GET_VERSION(wroot->link[1]); \
		if (!IS_EXTERNAL_NODE(wroot->link[0])) { \
			versions[4] = GET_VERSION(wroot->link[0]->link[0]); \
			versions[5] = GET_VERSION(wroot->link[0]->link[1]); \
		} \
		if (!IS_EXTERNAL_NODE(wroot->link[1])) { \
			versions[6] = GET_VERSION(wroot->link[1]->link[0]); \
			versions[7] = GET_VERSION(wroot->link[1]->link[1]); \
		} \
	} while (0)

#define DEL_VALIDATE_WIN_VERSION(wroot_parent, wroot, versions) \
	do { \
		if (versions[0] != GET_VERSION(wroot_parent) ||     \
		    versions[1] != GET_VERSION(wroot) ||            \
		    versions[2] != GET_VERSION(wroot->link[0]) ||   \
		    versions[3] != GET_VERSION(wroot->link[1]))     \
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR); \
		if (!IS_EXTERNAL_NODE(wroot->link[0])) { \
			if (versions[4] != GET_VERSION(wroot->link[0]->link[0]) || \
			    versions[5] != GET_VERSION(wroot->link[0]->link[1])) \
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR); \
		} \
		if (!IS_EXTERNAL_NODE(wroot->link[1])) { \
			if (versions[6] != GET_VERSION(wroot->link[1]->link[0]) || \
			    versions[7] != GET_VERSION(wroot->link[1]->link[1])) \
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR); \
		} \
	} while (0)

/**
 * Call it only when there is a short node in the tree.
 * Returns 0,1 or 2 depending on the rotations performed.
 **/
static int _delete_fix_violation(rbt_t *rbt, int key,
                                rbt_node_t **node_stack, int top,
                                htm_fg_tdata_t *tdata, int wroot_level,
                                rbt_node_t *root_parent)
{
	rbt_node_t *curr, *sibling, *parent, *gparent;
	int dir_from_parent, dir_from_gparent;

	if (IS_RED(node_stack[top])) {
		INC_VERSION(node_stack[top]);
		node_stack[top]->is_red = 0;
		return 0;
	}

	while (top > 0) {
		curr = node_stack[top--];
		if (IS_RED(curr)) {
			INC_VERSION(curr);
			curr->is_red = 0;
			return 0;
		}

		parent = node_stack[top--];
		dir_from_parent = parent->key < key;
		sibling = parent->link[!dir_from_parent];

		if (IS_RED(sibling)) {
			/* Case 1: RED sibling, reduce to a BLACK sibling case. */
			parent->is_red = 1;
			sibling->is_red = 0;
			gparent = node_stack[top];

			INC_VERSION(gparent);
			INC_VERSION(parent);
			INC_VERSION(sibling);

			dir_from_gparent = gparent->key < key;
			gparent->link[dir_from_gparent] = 
			            rbt_rotate_single(parent, dir_from_parent);
//			if (gparent == root_parent) {
			if (rbt->root == parent) {
//			if (wroot_level == 0) {
				rbt->root = gparent->link[dir_from_gparent];
				rbt->version++;
			}

			node_stack[++top] = sibling;
			sibling = parent->link[!dir_from_parent];
		}

		if (IS_BLACK(sibling->link[0]) && IS_BLACK(sibling->link[1])) {
			/* Case 2: BLACK sibling with two BLACK children. */
			INC_VERSION(parent);
			INC_VERSION(sibling);

			sibling->is_red = 1;
			node_stack[++top] = parent; /* new curr, is the parent. */
			continue;
		} else if (IS_RED(sibling->link[!dir_from_parent])) {
			/* Case 3: BLACK sibling with RED same direction child. */
			int parent_color = parent->is_red;

			gparent = node_stack[top];
			dir_from_gparent = gparent->key < key;

			INC_VERSION(gparent);
			INC_VERSION(parent);
			INC_VERSION(curr);
			INC_VERSION(sibling);
			INC_VERSION(sibling->link[0]);
			INC_VERSION(sibling->link[1]);

			gparent->link[dir_from_gparent] = 
			            rbt_rotate_single(parent, dir_from_parent);
//			if (gparent == root_parent) {
			if (rbt->root == parent) {
//			if (wroot_level == 0) {
				rbt->root = gparent->link[dir_from_gparent];
				rbt->version++;
			}
			gparent->link[dir_from_gparent]->is_red = parent_color;
			gparent->link[dir_from_gparent]->link[0]->is_red = 0;
			gparent->link[dir_from_gparent]->link[1]->is_red = 0;
			return 1;
		} else {
			/* Case 4: BLACK sibling with RED different direction child. */
			int parent_color = parent->is_red;

			gparent = node_stack[top];
			dir_from_gparent = gparent->key < key;

			INC_VERSION(gparent);
			INC_VERSION(parent);
			INC_VERSION(curr);
			INC_VERSION(sibling);
			INC_VERSION(sibling->link[0]);
			INC_VERSION(sibling->link[1]);

			gparent->link[dir_from_gparent] = 
			            rbt_rotate_double(parent, dir_from_parent);
//			if (gparent == root_parent) {
			if (rbt->root == parent) {
//			if (wroot_level == 0) {
				rbt->root = gparent->link[dir_from_gparent];
				rbt->version++;
			}
			gparent->link[dir_from_gparent]->is_red = parent_color;
			gparent->link[dir_from_gparent]->link[0]->is_red = 0;
			gparent->link[dir_from_gparent]->link[1]->is_red = 0;
			return 2;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static int delete_external_node(rbt_t *rbt, 
                                rbt_node_t *gparent, rbt_node_t *parent,
                                rbt_node_t *ext, int key,
                                rbt_node_t **nodes_to_delete, int wroot_level)
{
	if (ext->key != key)
		return 0;

	INC_VERSION(gparent);
	INC_VERSION(parent);
	INC_VERSION(parent->link[0]);
	INC_VERSION(parent->link[1]);

	int dir_from_gparent = gparent->key < key;
	int dir_from_parent = parent->key < key;

	nodes_to_delete[0] = ext;
	nodes_to_delete[1] = parent;
	gparent->link[dir_from_gparent] = parent->link[!dir_from_parent];

	if (parent == rbt->root) {
//	if (wroot_level == 0) {
		rbt->root = gparent->link[dir_from_parent];
		rbt->version++;
	}

	return 1;
}

static int _rbt_delete_helper_serial(rbt_t *rbt, int key,
                                     rbt_node_t **nodes_to_delete,
                                     htm_fg_tdata_t *tdata)
{
	rbt_node_t *node_stack[40];
	rbt_node_t *wroot, *wroot_parent, *curr, *left, *right,
	           *other, *next, *next_next, *next_other;
	rbt_node_t head = { 0 };
	int dir, i, j, top;
	int bb_sequence, deleted_node_is_red;
	int level = 0; /* The level of the window root. */

	if (!rbt->root)
		return 0;
	if (IS_EXTERNAL_NODE(rbt->root)) {
		if (rbt->root->key == key) {
			INC_VERSION(rbt->root);
			rbt->version++;
			nodes_to_delete[0] = rbt->root;
			rbt->root = NULL;
			return 1;
		}
		return 0;
	}
//	if (IS_BLACK(rbt->root) && IS_BLACK(rbt->root->link[0]) && 
//	                           IS_BLACK(rbt->root->link[1])) {
	if (IS_BLACK(rbt->root) &&
	    IS_BLACK(rbt->root->link[0]) && IS_BLACK(rbt->root->link[1]) &&
		!IS_EXTERNAL_NODE(rbt->root->link[0]) &&
	    IS_BLACK(rbt->root->link[0]->link[0]) &&
	    IS_BLACK(rbt->root->link[0]->link[1]) &&
		!IS_EXTERNAL_NODE(rbt->root->link[1]) &&
	    IS_BLACK(rbt->root->link[1]->link[0]) &&
	    IS_BLACK(rbt->root->link[1]->link[1])) {
		INC_VERSION(rbt->root);
		rbt->root->is_red = 1;
	}

	head.link[1] = rbt->root;
	wroot_parent = &head;
	wroot = rbt->root;

	while (1) {
		/* Window root should never be an external node. */
		assert(!IS_EXTERNAL_NODE(wroot));

		top = -1;
		node_stack[++top] = wroot_parent;
		node_stack[++top] = wroot;
		dir = wroot->key < key;
		curr = wroot->link[dir];

		bb_sequence = 0;
		while (bb_sequence < ACCESS_PATH_MAX_DEPTH) {
			if (IS_EXTERNAL_NODE(curr))
				break;
			if (IS_RED(curr))
				break;

			dir = curr->key < key;
			next = curr->link[dir];
			other = curr->link[!dir];

			if (IS_EXTERNAL_NODE(next))
				break;
			if (IS_RED(next) || IS_RED(other))
				break;

			/* Check if RED grandchild exists. */
			for (i=0; i < 2; i++)
				for (j=0; j < 2; j++)
					if (IS_RED(curr->link[i]->link[j]))
						break;

			/* If we reached here curr is BLACK with BLACK (grand)children. */
			bb_sequence++;
			if (bb_sequence >= ACCESS_PATH_MAX_DEPTH)
				break;

			node_stack[++top] = curr;
			curr = next;
		}

		node_stack[++top] = curr;

		if (IS_EXTERNAL_NODE(curr)) {
			deleted_node_is_red = node_stack[top-1]->is_red;
			if (delete_external_node(rbt, node_stack[top-2], node_stack[top-1],
			                         curr, key, nodes_to_delete, level)) {
				if (deleted_node_is_red == 0) {
					dir = node_stack[top-2]->key < key;
					node_stack[top-1] = node_stack[top-2]->link[dir];
					top--;
					_delete_fix_violation(rbt, key, node_stack, top,
					                      tdata, level, &head);
				}
				return 1;
			}
			return 0;
		}

		if (IS_RED(curr)) {
			wroot_parent = node_stack[top-1];
			wroot = curr;
			level += top - 1;
			continue;
		}

		dir = curr->key < key;
		next = curr->link[dir];
		other = curr->link[!dir];

		if (IS_EXTERNAL_NODE(next)) {
			deleted_node_is_red = curr->is_red;
			if (delete_external_node(rbt, node_stack[top-1], curr, next, key,
			                         nodes_to_delete, level)) {
				if (deleted_node_is_red == 0) {
					node_stack[top] = other;
					_delete_fix_violation(rbt, key, node_stack, top,
					                      tdata, level, &head);
				}
				return 1;
			}
			return 0;
		}

		if (IS_RED(next) || IS_RED(next->link[0]) ||
		                    IS_RED(next->link[1])) {
			wroot_parent = curr;
			wroot = next;
			level += top;
			continue;
		}
		if (IS_RED(other) || IS_RED(other->link[0]) ||
		                     IS_RED(other->link[1])) {
			wroot_parent = node_stack[top-1];
			wroot = curr;
			level += top - 1;
			continue;
		}

		INC_VERSION(curr);
		INC_VERSION(next);
		INC_VERSION(other);
		INC_VERSION(next->link[0]);
		INC_VERSION(next->link[1]);

		other->is_red = 1;
		next->is_red = 1;
		char ret = _delete_fix_violation(rbt, key, node_stack, top, tdata,
		                                 level, &head);
	
		dir = next->key < key;
		wroot_parent = curr;
		wroot = next;
		level += top + ret % 2; /* Even in double rotation curr moves one level
		                           down. */
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t **nodes_to_delete,
                              htm_fg_tdata_t *tdata)
{
	int retries, tx1_retries, tx2_retries;
	unsigned long long window_versions[9] = {0};
	rbt_node_t *node_stack[40];
	rbt_node_t *wroot, *wroot_parent, *curr, *left, *right,
	           *other, *next, *next_next, *next_other;
	rbt_node_t head = { 0 };
	int dir, i, j, top;
	int bb_sequence, deleted_node_is_red;
	int level = 0; /* The level of the window root. */

#	ifdef USE_CPU_LOCK
	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;
#	endif

	retries = -1;
try_from_scratch:
	level = 0;

	retries++;
	/* Grab the global lock and serialize. */
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner == tid) {
			cpu_locks[my_cpu_lock].owner = -1;
			pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
		}
#		endif
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_delete_helper_serial(rbt, key, nodes_to_delete, tdata);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	tx1_retries = -1;
TX1:
	tx1_retries++;
	if (tx1_retries >= TX_NUM_RETRIES) {
#		ifdef VERBOSE_STATISTICS
		tdata->lacqs_per_level[level]++;
#		endif
		goto try_from_scratch;
	}

	/* Avoid lemming effect. */
#	ifdef USE_CPU_LOCK
	while (cpu_locks[my_cpu_lock].owner > 0 &&
	       cpu_locks[my_cpu_lock].owner != tid &&
	       cpu_locks[my_cpu_lock].spinlock == 1)
		;
#	endif
	while (rbt->spinlock == 1)
		;

	head.is_red = 0;
	head.version = 1;

	tdata->tx_stats[2][0][0]++;
	tdata->tx_starts++;
	if (__builtin_tbegin(0)) {
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner > 0 &&
		    cpu_locks[my_cpu_lock].owner != tid &&
		    cpu_locks[my_cpu_lock].spinlock == 1)
			__builtin_tabort(0x77);
#		endif
		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

		if (!rbt->root) {
			__builtin_tend(0);
			return 0;
		}
		if (IS_EXTERNAL_NODE(rbt->root)) {
			if (rbt->root->key == key) {
				INC_VERSION(rbt->root);
				rbt->version++;
				nodes_to_delete[0] = rbt->root;
				rbt->root = NULL;
				__builtin_tend(0);
				return 1;
			}
			__builtin_tend(0);
			return 0;
		}
		if (IS_BLACK(rbt->root) && IS_BLACK(rbt->root->link[0]) && 
		                           IS_BLACK(rbt->root->link[1])) {
			int recolor_root = 1;
//			if ((!IS_EXTERNAL_NODE(rbt->root->link[0]) &&
//			    (IS_RED(rbt->root->link[0]->link[0]) ||
//			    IS_RED(rbt->root->link[0]->link[1]))) ||
//			    (!IS_EXTERNAL_NODE(rbt->root->link[1]) &&
//				(IS_RED(rbt->root->link[1]->link[0]) ||
//				 IS_RED(rbt->root->link[1]->link[1]))))
//				recolor_root = 0;

			int i, j, k;
			for (i=0; i < 2; i++) {
				if (recolor_root == 0)
					break;
				if (!IS_EXTERNAL_NODE(rbt->root->link[i])) {
					for (j=0; j < 2; j++) {
						if (recolor_root == 0)
							break;
						if (IS_RED(rbt->root->link[i]->link[j])) {
							recolor_root = 0;
							break;
						}
						if (!IS_EXTERNAL_NODE(rbt->root->link[i]->link[j])) {
							for (k=0; k < 2; k++) {
								if (IS_RED(rbt->root->link[i]->link[j]->link[k])) {
									recolor_root = 0;
									break;
								}
							}
						}
					}
				}
			}

			if (recolor_root) {
				rbt->version++;
				INC_VERSION(rbt->root);
				rbt->root->is_red = 1;
#				ifdef VERBOSE_STATISTICS
				tdata->restructures_at_level[0]++;
#				endif
			}
		}
	
		head.link[1] = rbt->root;
		wroot_parent = &head;
		wroot = rbt->root;
		DEL_GET_WIN_VERSION(wroot_parent, wroot, window_versions);
		__builtin_tend(0);
	} else {
#		ifdef VERBOSE_STATISTICS
		texasru_t texasru1 = __builtin_get_texasru();
		if (_TEXASRU_TRANSACTION_CONFLICT(texasru1))
			tdata->tx_con_aborts_per_level[level]++;
		else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru1))
			tdata->non_tx_con_aborts_per_level[level]++;
//		if (_TEXASRU_TRANSACTION_CONFLICT(texasru1))
//		if (_TEXASRU_ABORT(texasru1) && 
//		    _TEXASRU_FAILURE_CODE(texasru1) == IMPLICIT_ABORT_VERSION_ERROR)
			tdata->aborts_per_level[level]++;
#		endif

		ABORT_HANDLER(tdata, 2, 0);
		goto TX1;
	}

	while (1) {
		tx2_retries = -1;
TX2:
		tx2_retries++;
		if (tx2_retries >= TX_NUM_RETRIES) {
#			ifdef VERBOSE_STATISTICS
			tdata->lacqs_per_level[level]++;
#			endif
			goto try_from_scratch;
		}
#		ifdef USE_CPU_LOCK
		if (tx2_retries >= TX_NUM_RETRIES / 2 &&
		    cpu_locks[my_cpu_lock].owner != tid) {
			pthread_spin_lock(&cpu_locks[my_cpu_lock].spinlock);
			cpu_locks[my_cpu_lock].owner = tid;
		}
#		endif

		/* Avoid lemming effect. */
#		ifdef USE_CPU_LOCK
		while (cpu_locks[my_cpu_lock].owner > 0 &&
		       cpu_locks[my_cpu_lock].owner != tid &&
		       cpu_locks[my_cpu_lock].spinlock == 1)
			;
#		endif
		while (rbt->spinlock == 1)
			;

#		ifdef VERBOSE_STATISTICS
		tdata->starts_per_level[level]++;
#		endif
		tdata->tx_stats[2][1][0]++;
		tdata->tx_starts++;
		if (__builtin_tbegin(0)) {
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner > 0 &&
			    cpu_locks[my_cpu_lock].owner != tid &&
			    cpu_locks[my_cpu_lock].spinlock == 1)
				__builtin_tabort(0x77);
#			endif
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			/* Validate window and abort on failure. */
			DEL_VALIDATE_WIN_VERSION(wroot_parent, wroot, window_versions);

			/* Window root should never be an external node. */
			assert(!IS_EXTERNAL_NODE(wroot));
	
			top = -1;
			node_stack[++top] = wroot_parent;
			node_stack[++top] = wroot;

			curr = wroot;
			bb_sequence = 0;
	
			while (1) {
				dir = curr->key < key;
				curr = curr->link[dir];
				node_stack[++top] = curr;
	
				if (IS_EXTERNAL_NODE(curr)) {
					deleted_node_is_red = node_stack[top-1]->is_red;
					if (delete_external_node(rbt, node_stack[top-2], node_stack[top-1],
					                         curr, key, nodes_to_delete, level)) {
						if (deleted_node_is_red == 0) {
							dir = node_stack[top-2]->key < key;
							node_stack[top-1] = node_stack[top-2]->link[dir];
							top--;
							_delete_fix_violation(rbt, key, node_stack, top,
							                      tdata, level, &head);
						}
						__builtin_tend(0);
#						ifdef USE_CPU_LOCK
						if (cpu_locks[my_cpu_lock].owner == tid) {
							cpu_locks[my_cpu_lock].owner = -1;
							pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
						}
#						endif
						return 1;
					}
					__builtin_tend(0);
#					ifdef USE_CPU_LOCK
					if (cpu_locks[my_cpu_lock].owner == tid) {
						cpu_locks[my_cpu_lock].owner = -1;
						pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
					}
#					endif
					return 0;
				}
	
				/* If curr is RED or has one RED child or grandchild make it window root. */
				if (IS_RED(curr) || IS_RED(curr->link[0]) || IS_RED(curr->link[1]) ||
				    (!IS_EXTERNAL_NODE(curr->link[0]) &&
					 (IS_RED(curr->link[0]->link[0]) ||
				      IS_RED(curr->link[0]->link[1]))) ||
				    (!IS_EXTERNAL_NODE(curr->link[1]) &&
					 (IS_RED(curr->link[1]->link[0]) ||
				      IS_RED(curr->link[1]->link[1])))) {
					wroot_parent = node_stack[top-1];
					wroot = curr;
					level += top - 1;
					break;
				}
	
				if (IS_BLACK(curr) && IS_BLACK(curr->link[0]) && IS_BLACK(curr->link[1]) && 
				    (!IS_EXTERNAL_NODE(curr->link[0]) &&
					 IS_BLACK(curr->link[0]->link[0]) && 
				     IS_BLACK(curr->link[0]->link[1])) &&
				    (!IS_EXTERNAL_NODE(curr->link[1]) &&
					 IS_BLACK(curr->link[1]->link[0]) &&
				     IS_BLACK(curr->link[1]->link[1]))) {
					bb_sequence++;
					if (bb_sequence >= ACCESS_PATH_MAX_DEPTH) {
						top--;
						rbt_node_t *parent = node_stack[top];
						INC_VERSION(parent->link[0]);
						INC_VERSION(parent->link[1]);
						parent->link[0]->is_red = 1;
						parent->link[1]->is_red = 1;
						char ret = _delete_fix_violation(rbt, key, node_stack, top, tdata,
						                                 level, &head);
					
						dir = next->key < key;
						wroot_parent = parent;
						wroot = curr;
						level += top + ret % 2; /* Even in double rotation curr moves one level
						                           down. */
						break;
					}
				}
			}

			DEL_GET_WIN_VERSION(wroot_parent, wroot, window_versions);
			__builtin_tend(0);
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner == tid) {
				cpu_locks[my_cpu_lock].owner = -1;
				pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
			}
#			endif
		} else {
#			ifdef VERBOSE_STATISTICS
			texasru_t texasru1 = __builtin_get_texasru();
			if (_TEXASRU_TRANSACTION_CONFLICT(texasru1))
				tdata->tx_con_aborts_per_level[level]++;
			else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru1))
				tdata->non_tx_con_aborts_per_level[level]++;
//			if (_TEXASRU_TRANSACTION_CONFLICT(texasru1))
//			if (_TEXASRU_ABORT(texasru1) && 
//			    _TEXASRU_FAILURE_CODE(texasru1) == IMPLICIT_ABORT_VERSION_ERROR)
				tdata->aborts_per_level[level]++;
#			endif
			ABORT_HANDLER(tdata, 2, 1);
			goto TX2;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}


/******************************************************************************/

static int bh;
static int paths_with_bh_diff;
static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes, red_nodes, black_nodes;
static int red_red_violations, bst_violations;
static void _rbt_validate_rec(rbt_node_t *root, int _bh, int _th)
{
	if (!root)
		return;

	rbt_node_t *left = root->link[0];
	rbt_node_t *right = root->link[1];

	total_nodes++;
	black_nodes += (IS_BLACK(root));
	red_nodes += (IS_RED(root));
	_th++;
	_bh += (IS_BLACK(root));

	/* BST violation? */
	if (left && left->key > root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* Red-Red violation? */
	if (IS_RED(root) && (IS_RED(left) || IS_RED(right)))
		red_red_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (!left || !right) {
		total_paths++;
		if (bh == -1)
			bh = _bh;
		else if (_bh != bh)
			paths_with_bh_diff++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_rbt_validate_rec(left, _bh, _th);
	if (right)
		_rbt_validate_rec(right, _bh, _th);
}

static inline int _rbt_validate_helper(rbt_node_t *root)
{
	int check_bh = 0, check_red_red = 0, check_bst = 0;
	int check_rbt = 0;
	bh = -1;
	paths_with_bh_diff = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = black_nodes = red_nodes = 0;
	red_red_violations = 0;
	bst_violations = 0;

	_rbt_validate_rec(root, 0, 0);

	check_bh = (paths_with_bh_diff == 0);
	check_red_red = (red_red_violations == 0);
	check_bst = (bst_violations == 0);
	check_rbt = (check_bh && check_red_red && check_bst);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Valid Red-Black Tree: %s\n",
	       check_rbt ? "Yes [OK]" : "No [ERROR]");
	printf("  Black height: %d [%s]\n", bh,
	       check_bh ? "OK" : "ERROR");
	printf("  Red-Red Violation: %s\n",
	       check_red_red ? "No [OK]" : "Yes [ERROR]");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size (Total / Black / Red): %8d / %8d / %8d\n",
	       total_nodes, black_nodes, red_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_rbt;
}

static rbt_node_t *_rbt_warmup_insert_rec(rbt_t *rbt, rbt_node_t *root, 
                                   rbt_node_t *node[2], int *found, int level)
{
	if (!root) {
		root = node[0];
		return root;
	}

	if (IS_EXTERNAL_NODE(root) && root->key == node[0]->key) {
		*found = 1;
		return root;
	}

	if (IS_EXTERNAL_NODE(root)) {
		root->link[0] = node[0];
		root->link[1] = node[1];
		root->is_red = 1;
		root->link[0]->is_red = 0;
		root->link[1]->is_red = 0;

		if (root->key > node[0]->key) {
			root->link[1]->key = root->key;
			root->key = root->link[0]->key;
		} else {
			root->link[0]->key = root->key;
		}

		return root;
	}

	int dir = root->key < node[0]->key;
	rbt_node_t *new_link_dir = _rbt_warmup_insert_rec(rbt, root->link[dir],
	                                           node, found, level + 1);

	if (root->link[dir] != new_link_dir)
		root->link[dir] = new_link_dir;

	/* DEBUG */
	if (*found)
		return root;

	/* If we caused a Red-Red violation let's fix it. */
	if (IS_BLACK(root->link[dir]))
		return root;

	if (IS_BLACK(root->link[dir]->link[0]) && 
	    IS_BLACK(root->link[dir]->link[1]))
		return root;

	/* root->link[dir] is red with one red child. */
	if (IS_RED(root->link[!dir])) {
		/* Case 1 */
		root->is_red = 1;
		root->link[dir]->is_red = 0;
		root->link[!dir]->is_red = 0;
	} else if (IS_BLACK(root->link[!dir])) {
		if (IS_RED(root->link[dir]->link[dir])) {
			/* Case 2 */
			root->is_red = 1;
			root->link[dir]->is_red = 0;
			root = rbt_rotate_single(root, !dir);
		} else {
			/* Case 3 */
			root->is_red = 1;
			root->link[dir]->link[!dir]->is_red = 0;
			root = rbt_rotate_double(root, !dir);
		}
	}

	return root;
}

static inline int _rbt_warmup_insert_helper(rbt_t *rbt, rbt_node_t *nodes[2])
{
	int found = 0;

	rbt_node_t *new_root = _rbt_warmup_insert_rec(rbt, rbt->root, nodes,
	                                       &found, 0);
	if (rbt->root != new_root)
		rbt->root = new_root;
	if (IS_RED(rbt->root))
		rbt->root->is_red = 0;

	return !found;
}

static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	rbt_node_t *nodes[2];
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		nodes[0] = rbt_node_new(key, NULL);
		nodes[1] = rbt_node_new(key, NULL);

		ret = _rbt_warmup_insert_helper(rbt, nodes);
		nodes_inserted += ret;

		if (!ret) {
			free(nodes[0]);
			free(nodes[1]);
		}
	}

	return nodes_inserted;
}

/* DEBUG */
static void rbt_print_rec(rbt_node_t *root, int level)
{
	int i;

	if (root)
		rbt_print_rec(root->link[1], level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%d[%s] {ver: %llu}\n", root->key, root->is_red ? "RED" : "BLA",
	                               GET_VERSION(root));

	rbt_print_rec(root->link[0], level + 1);
}

static void rbt_print_struct(rbt_t *rbt)
{
	if (rbt->root == NULL)
		printf("[empty]");
	else
		rbt_print_rec(rbt->root, 0);
	printf("\n");
}

static int rbt_find_loop(rbt_node_t *root, int level)
{
	int ret;

	if (root) {
		if (root->link[0] == root || root->link[1] == root) {
			printf("Found loop at level %d (q = %d %d)\n", level, root->key, root->is_red);
			return 1;
		} else if (!IS_EXTERNAL_NODE(root) && (!root->link[0] || !root->link[1])) {
			printf("Found node with one child at level %d\n", level);
			return 2;
		}
	}

	if (!root) {
		return 0;
	}

	if (rbt_find_loop(root->link[0], level + 1))
		return 1;
	if (rbt_find_loop(root->link[1], level + 1))
		return 1;
	return 0;
}
static int _rbt_has_loop(rbt_t *rbt)
{
	return rbt_find_loop(rbt->root, 0);
}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(rbt_node_t));
	printf("Size of tree is %lu\n", sizeof(rbt_t));
	return _rbt_new_helper();
}

void *rbt_thread_data_new(int tid)
{
	return htm_fg_tdata_new(tid);
}

void rbt_thread_data_print(void *thread_data)
{
	htm_fg_tdata_print(thread_data);
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
	htm_fg_tdata_add(d1, d2, dst);
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret;

	ret = _rbt_lookup_helper(rbt, thread_data, key, thread_data);

	return ret;
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret;
	rbt_node_t *nodes[2];

	nodes[0] = rbt_node_new(key, value);
	nodes[1] = rbt_node_new(key, value);

	ret = _rbt_insert_helper(rbt, nodes, thread_data);

	if (!ret) {
		free(nodes[0]);
		free(nodes[1]);
	}

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret;
	rbt_node_t *nodes_to_delete[2];

	ret = _rbt_delete_helper(rbt, key, nodes_to_delete, thread_data);

//	if (ret) {
//		free(nodes_to_delete[0]);
//		free(nodes_to_delete[1]);
//	}

	return ret;
}

int rbt_validate(void *rbt)
{
	int ret;
	ret = _rbt_validate_helper(((rbt_t *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret;
	ret = _rbt_warmup_helper((rbt_t *)rbt, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	char *str;
	XMALLOC(str, 100);
	sprintf(str, "links_td_tarjan_external_fg_htm ( ACCESS_PATH_MAX_DEPTH: %d )\n",
	        ACCESS_PATH_MAX_DEPTH);
	return str;
}

void rbt_print(void *rbt)
{
	rbt_print_struct(rbt);
}

int rbt_has_loop(void *rbt)
{
	return _rbt_has_loop(rbt);
}
