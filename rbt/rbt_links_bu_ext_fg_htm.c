#include <htmintrin.h> /* power8 tm gcc intrinsics. */
#include <assert.h>
#include <pthread.h>  /* pthread_spinlock_t */

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

//#include "rbt_links_bu_ext_fg_htm_thread_data.h"
#define TX_STATS_ARRAY_NR_TRANS 3
#include "rbt_links_td_ext_fg_htm_thread_data.h"

#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )
#define IS_BLACK(node) ( !(node) || !(node)->is_red )
#define IS_RED(node) ( !IS_BLACK(node) )
#define GET_VERSION(node) ( (!(node)) ? 0 : (node)->version )
#define INC_VERSION(node) (node)->version++

#define IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN 0xfe
#define IMPLICIT_ABORT_VERSION_ERROR 0xee

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

/* >= 1 for progress. */
#define TRAVERSAL_TX_PATH_SIZE 1
#if TRAVERSAL_TX_PATH_SIZE==0
#	error "TRAVERSAL_TX_PATH_SIZE cannot be 0"
#endif

#ifdef USE_CPU_LOCK
#	define NR_CPUS 20
#endif

typedef struct rbt_node {
	int is_red;
	int key;
	void *value;
	struct rbt_node *link[2];

	unsigned long long version; /* A version number, starts from 1. */

	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) - 
	             2 * sizeof(struct rbt_node *) - sizeof(unsigned long long)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;

typedef struct {
	rbt_node_t *root;
	unsigned long long version;

	pthread_spinlock_t spinlock; /* Used as htm fallback */

} rbt_t;

#ifdef USE_CPU_LOCK
struct {
	pthread_spinlock_t spinlock;
	int owner; /* A per cpu lock. Its value is the pid of the owner. */
	char padding[CACHE_LINE_SIZE - sizeof(int) - sizeof(pthread_spinlock_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) cpu_locks[NR_CPUS];
#endif

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
	int ret = 0;
	rbt_node_t *curr = rbt->root;

	//> Empty tree.
	if (!curr)
		return 0;

	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < key;
		curr = curr->link[dir];
	}

	if (curr->key == key)
		ret = 1;

	return ret;
}

static int _rbt_lookup_helper(rbt_t *rbt, htm_fg_tdata_t *tdata, int key)
{
	int dir, found;
	rbt_node_t *curr;
	unsigned long long window_versions[1]; /* curr version */
	int retries = -1;
	int tx1_retries, tx2_retries;

#	ifdef USE_CPU_LOCK
	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;
#	endif

try_from_scratch:

	retries++;

	/* Global lock fallback. */
	if (retries >= TX_NUM_RETRIES) {
		tdata->tx_lacqs++;
		int ret = 0;
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
		tx2_retries = -1; /* Reset the retries for the next window. */
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

//			/* External node reached. */
//			if (IS_EXTERNAL_NODE(curr)) {
//				found = (curr->key == key);
//				__builtin_tend(0);
//				return found;
//			}
//
//			/* Move to the next node. */
//			dir = curr->key < key;
//			curr = curr->link[dir];

			/* Walk down to the next nodes. */
			int steps = 0;
			while (!IS_EXTERNAL_NODE(curr) && steps < TRAVERSAL_TX_PATH_SIZE) {
				dir = curr->key < key;
				curr = curr->link[dir];
				steps++;
			}

			if (IS_EXTERNAL_NODE(curr)) {
				found = (curr->key == key);
				__builtin_tend(0);
				return found;
			}

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

/**
 * Replace an external node with a RED internal with two children.
 * Example (* means red node);
 *
 *       8                4*
 *     /   \     =>     /    \
 *   NULL NULL         4      8
 *                   /   \  /   \
 *                 NULL  NULL  NULL
 **/
static inline void replace_external_node(rbt_node_t *root,
                                         rbt_node_t *nodes[2])
{
	root->link[0] = nodes[0];
	root->link[1] = nodes[1];
	root->is_red = 1;
	root->link[0]->is_red = 0;
	root->link[1]->is_red = 0;

	if (root->key > nodes[0]->key) {
		root->link[1]->key = root->key;
		root->key = root->link[0]->key;
	} else {
		root->link[0]->key = root->key;
	}
}

/*
 * 'top' shows at the last occupied index of 'node_stack' which is the newly
 * added node to the tree and is RED.
 */
static inline void _rbt_insert_fixup(rbt_t *rbt, int key,
                                     rbt_node_t **node_stack,
                                     unsigned long long *stack_versions,
                                     int top)
{
	rbt_node_t *ggparent, *gparent, *parent, *uncle;

	assert(IS_RED(node_stack[top]));

	/**
	 * I don't know why but this check is necessary.
	 * I could not figure why, but without this, rbt errors occur.
	 **/
	if (stack_versions[0] != GET_VERSION(node_stack[0]))
		__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

	/* Consume the newly inserted RED node from the stack. */
	top--;

	while (top >= 0) {
		parent = node_stack[top--];
		if (stack_versions[top+1] != GET_VERSION(parent))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		/* parent is BLACK, we are done. */
		if (IS_BLACK(parent))
			break;

		/* parent is RED so it cannot be root => it must have a parent. */
		gparent = node_stack[top--];
		if (stack_versions[top+1] != GET_VERSION(gparent))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		/* What is the direction we followed from gparent to parent? */
		int dir = gparent->key < key;
		uncle = gparent->link[!dir];

		ggparent = NULL;
		if (top >= 0) {
			ggparent = node_stack[top];
			if (stack_versions[top] != GET_VERSION(ggparent))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
		}

		if (IS_RED(uncle)) {              /* Case 1 (Recolor and move up) */
			gparent->is_red = 1;
			parent->is_red = 0;
			uncle->is_red = 0;

			if (ggparent) {
				INC_VERSION(ggparent);
				stack_versions[top] = GET_VERSION(ggparent);
			}
			INC_VERSION(gparent);
			INC_VERSION(parent);
			INC_VERSION(uncle);
			continue;
		} else {
			ggparent = (top >= 0) ? node_stack[top] : NULL;
			int dir_from_parent = parent->key < key;

			if (stack_versions[top] != GET_VERSION(ggparent))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			INC_VERSION(gparent);
			INC_VERSION(parent);
			if (dir == dir_from_parent) { /* Case 2 (Single rotation) */
				gparent->is_red = 1;
				parent->is_red = 0;
				if (ggparent) {
					INC_VERSION(ggparent);
					int dir_from_ggparent = ggparent->key < key;
					ggparent->link[dir_from_ggparent] = 
					          rbt_rotate_single(gparent, !dir);
				 } else {
					rbt->version++;
					rbt->root = rbt_rotate_single(gparent, !dir);
				 }
			} else {                      /* Case 3 (Double rotation) */
				gparent->is_red = 1;
				parent->link[dir_from_parent]->is_red = 0;
				if (ggparent) {
					INC_VERSION(ggparent);
					int dir_from_ggparent = ggparent->key < key;
					ggparent->link[dir_from_ggparent] = 
					          rbt_rotate_double(gparent, !dir);
				} else {
					rbt->version++;
					rbt->root = rbt_rotate_double(gparent, !dir);
				}
			}
			break;
		}
	}

	if (IS_RED(rbt->root)) {
		rbt->root->is_red = 0;
		rbt->version++;
	}
}

#define MAX_PATH_LEN 40

static inline int _rbt_insert_helper_serial(rbt_t *rbt, rbt_node_t *nodes[2])
{
	unsigned long long stack_versions[MAX_PATH_LEN];
	rbt_node_t *node_stack[MAX_PATH_LEN], *curr;
	int top = -1;

	/* Empty tree */
	if (!rbt->root) {
		rbt->root = nodes[0];
		rbt->root->is_red = 0;
		return 1;
	}

	/* Traverse the tree until an external node is reached. */
	curr = rbt->root;
	node_stack[++top] = curr;
	stack_versions[top] = GET_VERSION(curr);
	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < nodes[0]->key;
		curr = curr->link[dir];
		node_stack[++top] = curr;
		INC_VERSION(curr);
		stack_versions[top] = GET_VERSION(curr);
	}

	/* Did we find the external node we were looking for? */
	if (curr->key == nodes[0]->key)
		return 0;

	/* Insert the new node and fixup any violations. */
	replace_external_node(curr, nodes);
	_rbt_insert_fixup(rbt, nodes[0]->key, node_stack, stack_versions, top);

	return 1;
}

static inline int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *nodes[2],
                                     htm_fg_tdata_t *tdata)
{
	rbt_node_t *node_stack[MAX_PATH_LEN], *curr;
	unsigned long long stack_versions[MAX_PATH_LEN];
	int top = -1, i;
	int retries = -1;
	int insert_fixup_retries = -1, window_retries = -1;

#	ifdef USE_CPU_LOCK
	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;
#	endif

try_from_scratch:
	
	retries++;
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner == tid)
			pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
#		endif
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_insert_helper_serial(rbt, nodes);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	top = -1;

	/* First transaction at the root. */
#	ifdef USE_CPU_LOCK
	while (cpu_locks[my_cpu_lock].owner > 0 &&
	       cpu_locks[my_cpu_lock].owner != tid &&
	       cpu_locks[my_cpu_lock].spinlock == 1)
		;
#	endif

	while (rbt->spinlock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[1][0][0]++;
	if (__builtin_tbegin(0)) {
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner > 0 &&
		    cpu_locks[my_cpu_lock].owner != tid &&
		    cpu_locks[my_cpu_lock].spinlock == 1)
			__builtin_tabort(0x77);
#		endif

		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

		/* Empty tree */
		if (!rbt->root) {
			rbt->root = nodes[0];
			rbt->root->is_red = 0;

			INC_VERSION(rbt->root);
			rbt->version++;
			__builtin_tend(0);
			return 1;
		}

		curr = rbt->root;
		node_stack[++top] = curr;
		stack_versions[top] = GET_VERSION(curr);

		__builtin_tend(0);
	} else {
		tdata->tx_aborts++;
		tdata->tx_stats[1][0][1]++;

		texasru_t texasru = __builtin_get_texasru();
		if (_TEXASRU_ABORT(texasru) && 
		    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
			tdata->tx_stats[1][0][5]++;
			tdata->tx_aborts_version_error++;
		} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
			tdata->tx_stats[1][0][4]++;
			tdata->tx_aborts_footprint_overflow++;
		} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
			tdata->tx_stats[1][0][2]++;
			tdata->tx_aborts_transaction_conflict++;
		} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
			tdata->tx_stats[1][0][3]++;
			tdata->tx_aborts_non_transaction_conflict++;
		} else {
			tdata->tx_stats[1][0][6]++;
			tdata->tx_aborts_rest++;
		}

		tdata->tx_stats[1][0][7]++;
		goto try_from_scratch;
	}

	/* Window transactions until an external node is reached. */
	while (1) {
		int capacity_window_retries = -1;
		window_retries = -1;

retry_window:
		window_retries++;
		if (window_retries >= TX_NUM_RETRIES) {
			tdata->tx_stats[1][1][7]++;
			goto try_from_scratch;
		}

#		ifdef USE_CPU_LOCK
		while (cpu_locks[my_cpu_lock].owner > 0 && 
		       cpu_locks[my_cpu_lock].owner != tid &&
		       cpu_locks[my_cpu_lock].spinlock == 1)
			;

		if (capacity_window_retries >= 1 &&
		    cpu_locks[my_cpu_lock].owner < 0) {
			pthread_spin_lock(&cpu_locks[my_cpu_lock].spinlock);
			cpu_locks[my_cpu_lock].owner = tid;
		}
#		endif

		while (rbt->spinlock == 1)
			;

		tdata->tx_starts++;
		tdata->tx_stats[1][1][0]++;
		if (__builtin_tbegin(0)) {
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner > 0 && 
			    cpu_locks[my_cpu_lock].owner != tid &&
			    cpu_locks[my_cpu_lock].spinlock == 1)
				__builtin_tabort(0x77);
#			endif

			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			/* Check that window version is unchanged. */
			if (stack_versions[top] != GET_VERSION(curr))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			/* Traverse as deep as we want. */
			for (i=0; i < TRAVERSAL_TX_PATH_SIZE; i++) {
				if (IS_EXTERNAL_NODE(curr))
					break;

				/* Go to the next node. */
				int dir = curr->key < nodes[0]->key;
				curr = curr->link[dir];
				node_stack[++top] = curr;
				stack_versions[top] = GET_VERSION(curr);
			}

			/* Did we find the external node we were looking for? */
			if (IS_EXTERNAL_NODE(curr)) {
				if (curr->key == nodes[0]->key) {
					__builtin_tend(0);
#					ifdef USE_CPU_LOCK
					if (cpu_locks[my_cpu_lock].owner == tid) {
						cpu_locks[my_cpu_lock].owner = -1;
						pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
					}
#					endif
					return 0;
				}
				__builtin_tend(0);
#				ifdef USE_CPU_LOCK
				if (cpu_locks[my_cpu_lock].owner == tid) {
					cpu_locks[my_cpu_lock].owner = -1;
					pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
				}
#				endif
				break;
			}

			__builtin_tend(0);
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner == tid) {
				cpu_locks[my_cpu_lock].owner = -1;
				pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
			}
#			endif


			continue;
		} else {
			tdata->tx_aborts++;
			tdata->tx_stats[1][1][1]++;

			texasru_t texasru = __builtin_get_texasru();
			if (_TEXASRU_ABORT(texasru) && 
			    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
				tdata->tx_stats[1][1][5]++;
#				ifdef USE_CPU_LOCK
				if (cpu_locks[my_cpu_lock].owner == tid) {
					cpu_locks[my_cpu_lock].owner = -1;
					pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
				}
#				endif
				tdata->tx_aborts_version_error++;
				goto try_from_scratch;
			} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
				tdata->tx_stats[1][1][4]++;
				tdata->tx_aborts_footprint_overflow++;
				capacity_window_retries++;
			} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
				tdata->tx_stats[1][1][2]++;
				tdata->tx_aborts_transaction_conflict++;
			} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
				tdata->tx_stats[1][1][3]++;
				tdata->tx_aborts_non_transaction_conflict++;
			} else {
				tdata->tx_stats[1][1][6]++;
				tdata->tx_aborts_rest++;
			}
			goto retry_window;
		}
	}

	insert_fixup_retries = -1;
retry_insertion:

	insert_fixup_retries++;
//	if (insert_fixup_retries >= 2 * TX_NUM_RETRIES) {
	if (insert_fixup_retries >= TX_NUM_RETRIES) {
		tdata->tx_stats[1][2][7]++;
		goto try_from_scratch;
	}

#	ifdef USE_CPU_LOCK
	while (cpu_locks[my_cpu_lock].owner > 0 &&
	       cpu_locks[my_cpu_lock].owner != tid &&
	       cpu_locks[my_cpu_lock].spinlock == 1)
		;

	if (insert_fixup_retries >= TX_NUM_RETRIES &&
	    cpu_locks[my_cpu_lock].owner < 0) {
		pthread_spin_lock(&cpu_locks[my_cpu_lock].spinlock);
		cpu_locks[my_cpu_lock].owner = tid;
	}
#	endif

	/* Last transaction to insert the node and fixup. */
	while (rbt->spinlock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[1][2][0]++;
	if (__builtin_tbegin(0)) {
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner > 0 &&
		    cpu_locks[my_cpu_lock].owner != tid &&
		    cpu_locks[my_cpu_lock].spinlock == 1)
			__builtin_tabort(0x77);
#		endif

		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
		/* Check that window version is unchanged. */
		if (stack_versions[top] != GET_VERSION(curr))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		/* Insert the new node and fixup any violations. */
		replace_external_node(curr, nodes);
		INC_VERSION(curr);

		_rbt_insert_fixup(rbt, nodes[0]->key, node_stack, stack_versions, top);
		__builtin_tend(0);
	} else {
		tdata->tx_aborts++;
		tdata->tx_stats[1][2][1]++;
		texasru_t texasru = __builtin_get_texasru();
		if (_TEXASRU_ABORT(texasru) && 
		    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
			tdata->tx_stats[1][2][5]++;
			tdata->tx_aborts_version_error++;
			goto try_from_scratch;
		} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
			tdata->tx_stats[1][2][4]++;
			tdata->tx_aborts_footprint_overflow++;
		} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
			tdata->tx_stats[1][2][2]++;
			tdata->tx_aborts_transaction_conflict++;
		} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
			tdata->tx_stats[1][2][3]++;
			tdata->tx_aborts_non_transaction_conflict++;
		} else {
			tdata->tx_stats[1][2][6]++;
			tdata->tx_aborts_rest++;
		}
		goto retry_insertion;
	}

#	ifdef USE_CPU_LOCK
	if (cpu_locks[my_cpu_lock].owner == tid) {
		cpu_locks[my_cpu_lock].owner = -1;
		pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
	}
#	endif

	return 1;
}

/*
 * The top of the stack contains the doubly-black node.
 */
static inline void _rbt_delete_fixup(rbt_t *rbt, int key,
                                     rbt_node_t **node_stack, 
                                     unsigned long long *stack_versions,
                                     int top)
{
	rbt_node_t *curr, *sibling, *parent, *gparent;
	int dir_from_parent, dir_from_gparent;

	while (top > 0) {
		curr = node_stack[top--];
		if (stack_versions[top+1] != GET_VERSION(curr))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		parent = node_stack[top--];
		if (stack_versions[top+1] != GET_VERSION(parent))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		if (top >= 0) {
			gparent = node_stack[top];
			if (stack_versions[top] != GET_VERSION(gparent))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
		}

		if (IS_RED(curr)) {
			curr->is_red = 0;
			INC_VERSION(parent);
			INC_VERSION(curr);
			return;
		}

		dir_from_parent = parent->key < key;
		sibling = parent->link[!dir_from_parent];

		if (IS_RED(sibling)) {
			/* Case 1: RED sibling, reduce to a BLACK sibling case. */
			INC_VERSION(parent);
			INC_VERSION(sibling);

			parent->is_red = 1;
			sibling->is_red = 0;
			gparent = (top >= 0) ? node_stack[top] : NULL;
			if (stack_versions[top] != GET_VERSION(gparent))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR - 6);

			if (gparent) {
				INC_VERSION(gparent);
				dir_from_gparent = gparent->key < key;
				gparent->link[dir_from_gparent] = 
				            rbt_rotate_single(parent, dir_from_parent);
				stack_versions[top] = GET_VERSION(gparent);
			} else {
				rbt->root++;
				rbt->root = rbt_rotate_single(parent, dir_from_parent);
			}

			node_stack[++top] = sibling;
			stack_versions[top] = GET_VERSION(sibling);

			sibling = parent->link[!dir_from_parent];
		}

		if (IS_BLACK(sibling->link[0]) && IS_BLACK(sibling->link[1])) {
			/* Case 2: BLACK sibling with two BLACK children. */
			INC_VERSION(sibling);

			sibling->is_red = 1;
			node_stack[++top] = parent; /* new curr, is the parent. */
			stack_versions[top] = GET_VERSION(parent);
		} else if (IS_RED(sibling->link[!dir_from_parent])) {
			/* Case 3: BLACK sibling with RED same direction child. */
			INC_VERSION(parent);
			INC_VERSION(sibling);
			INC_VERSION(sibling->link[0]);
			INC_VERSION(sibling->link[1]);

			int parent_color = parent->is_red;
			rbt_node_t *new_parent = NULL;

			gparent = (top >= 0) ? node_stack[top] : NULL;
			if (gparent) {
				INC_VERSION(gparent);

				dir_from_gparent = gparent->key < key;
				gparent->link[dir_from_gparent] = 
				            rbt_rotate_single(parent, dir_from_parent);
				new_parent = gparent->link[dir_from_gparent];
			} else {
				rbt->version++;

				rbt->root = rbt_rotate_single(parent, dir_from_parent);
				new_parent = rbt->root;
			}
			new_parent->is_red = parent_color;
			new_parent->link[0]->is_red = 0;
			new_parent->link[1]->is_red = 0;
			return;
		} else {
			/* Case 4: BLACK sibling with RED different direction child. */
			INC_VERSION(parent);
			INC_VERSION(sibling);
			INC_VERSION(sibling->link[0]);
			INC_VERSION(sibling->link[1]);

			int parent_color = parent->is_red;
			rbt_node_t *new_parent = NULL;

			gparent = (top >= 0) ? node_stack[top] : NULL;
			if (gparent) {
				INC_VERSION(gparent);

				dir_from_gparent = gparent->key < key;
				gparent->link[dir_from_gparent] = 
				            rbt_rotate_double(parent, dir_from_parent);
				new_parent = gparent->link[dir_from_gparent];
			} else {
				rbt->version++;

				rbt->root = rbt_rotate_double(parent, dir_from_parent);
				new_parent = rbt->root;
			}
			new_parent->is_red = parent_color;
			new_parent->link[0]->is_red = 0;
			new_parent->link[1]->is_red = 0;
			return;
		}
	}
}

static inline int _rbt_delete_helper_serial(rbt_t *rbt, int key, 
                                            rbt_node_t *nodes_to_delete[2])
{
	rbt_node_t *node_stack[MAX_PATH_LEN], *curr;
	unsigned long long stack_versions[MAX_PATH_LEN];
	rbt_node_t *parent, *gparent;
	int top = -1;

	/* Empty tree */
	if (!rbt->root) {
		return 0;
	}

	/* Traverse the tree until an external node is reached. */
	curr = rbt->root;
	node_stack[++top] = curr;
	stack_versions[top] = GET_VERSION(curr);
	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < key;
		curr = curr->link[dir];
		node_stack[++top] = curr;
		INC_VERSION(curr);
		stack_versions[top] = GET_VERSION(curr);
	}

	/* Key not in the tree. */
	if (curr->key != key)
		return 0;

	/* Key found. Delete the node and fixup any violations. */
	parent = (top >= 1) ? node_stack[top-1] : NULL;
	gparent = (top >= 2) ? node_stack[top-2] : NULL;

	if (!parent) { /* Only one node in the tree. */
		nodes_to_delete[0] = curr;
		nodes_to_delete[1] = NULL;
		rbt->root = NULL;
		INC_VERSION(curr);
		rbt->version++;
		return 1; /* No fixup necessary. */
	} else if (!gparent) { /* We don't have gparent so parent is the root. */
		int dir_from_parent = parent->key < key;

		rbt->root = parent->link[!dir_from_parent];
		nodes_to_delete[0] = parent;
		nodes_to_delete[1] = curr;
		INC_VERSION(parent);
		rbt->version++;
		return 1; /* No fixup necessary. */
	} else {
		INC_VERSION(curr);
		INC_VERSION(parent);
		INC_VERSION(gparent);

		/* Refresh the version of the gparent (curr, and parent are 
		   going to be removed). */
		stack_versions[top-2] = GET_VERSION(gparent);

		int dir_from_gparent = gparent->key < key;
		int dir_from_parent = parent->key < key;

		gparent->link[dir_from_gparent] = parent->link[!dir_from_parent];
		nodes_to_delete[0] = parent;
		nodes_to_delete[1] = curr;

		/* If parent was BLACK we need to fixup.
		   First remove parent, curr from the stack and and curr's sibling.
		 */
		top--;
		node_stack[top] = parent->link[!dir_from_parent];
		stack_versions[top] = GET_VERSION(parent->link[!dir_from_parent]);

		if (IS_BLACK(parent))
			_rbt_delete_fixup(rbt, key, node_stack, stack_versions, top);

		return 1;
	}

	/* Unreachable */
	return 0;
}

static inline int _rbt_delete_helper(rbt_t *rbt, int key,
                                     rbt_node_t *nodes_to_delete[2],
                                     htm_fg_tdata_t *tdata)
{
	rbt_node_t *node_stack[MAX_PATH_LEN], *curr;
	rbt_node_t *parent, *gparent;
	unsigned long long stack_versions[MAX_PATH_LEN];
	int top = -1, i;
	int retries = -1, window_retries = -1, delete_fixup_retries = -1;

#	ifdef USE_CPU_LOCK
	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;
#	endif

try_from_scratch:
	retries++;
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
#		ifdef USE_CPU_LOCK
		/* FIXME free the cpu_lock_here? */
		if (cpu_locks[my_cpu_lock].owner == tid)
			pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
#		endif
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_delete_helper_serial(rbt, key, nodes_to_delete);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	top = -1;

	/* First transaction at the root. */
#	ifdef USE_CPU_LOCK
	while (cpu_locks[my_cpu_lock].owner > 0 && 
	       cpu_locks[my_cpu_lock].owner != tid &&
	       cpu_locks[my_cpu_lock].spinlock == 1)
		;
#	endif

	while (rbt->spinlock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[2][0][0]++;
	if (__builtin_tbegin(0)) {
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner > 0 &&
		    cpu_locks[my_cpu_lock].owner != tid &&
		    cpu_locks[my_cpu_lock].spinlock == 1)
			__builtin_tabort(0x77);
#		endif

		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

		/* Empty tree */
		if (!rbt->root) {
			__builtin_tend(0);
			return 0;
		}

		curr = rbt->root;
		node_stack[++top] = curr;
		stack_versions[top] = GET_VERSION(curr);

		__builtin_tend(0);
	} else {
		tdata->tx_aborts++;
		tdata->tx_stats[2][0][1]++;

		texasru_t texasru = __builtin_get_texasru();
		if (_TEXASRU_ABORT(texasru) && 
		    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
			tdata->tx_stats[2][0][5]++;
			tdata->tx_aborts_version_error++;
		} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
			tdata->tx_stats[2][0][4]++;
			tdata->tx_aborts_footprint_overflow++;
		} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
			tdata->tx_stats[2][0][2]++;
			tdata->tx_aborts_transaction_conflict++;
		} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
			tdata->tx_stats[2][0][3]++;
			tdata->tx_aborts_non_transaction_conflict++;
		} else {
			tdata->tx_stats[2][0][6]++;
			tdata->tx_aborts_rest++;
		}
		tdata->tx_stats[2][0][7]++;
		goto try_from_scratch;
	}

	/* Window transactions until an external node is reached. */
	while (1) {
		window_retries = -1;

retry_window:
		window_retries++;
		if (window_retries >= TX_NUM_RETRIES) {
			tdata->tx_stats[2][1][7]++;
			goto try_from_scratch;
		}

#		ifdef USE_CPU_LOCK
		while (cpu_locks[my_cpu_lock].owner > 0 &&
		       cpu_locks[my_cpu_lock].owner != tid &&
		       cpu_locks[my_cpu_lock].spinlock == 1)
			;
#		endif

		while (rbt->spinlock == 1)
			;

		tdata->tx_starts++;
		tdata->tx_stats[2][1][0]++;
		if (__builtin_tbegin(0)) {
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner > 0 &&
			    cpu_locks[my_cpu_lock].owner != tid &&
			    cpu_locks[my_cpu_lock].spinlock == 1)
				__builtin_tabort(0x77);
#			endif

			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			/* Check that window version is unchanged. */
			if (stack_versions[top] != GET_VERSION(curr))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			/* Traverse as deep as we want. */
			for (i=0; i < TRAVERSAL_TX_PATH_SIZE; i++) {
				if (IS_EXTERNAL_NODE(curr))
					break;

				/* Go to the next node. */
				int dir = curr->key < key;
				curr = curr->link[dir];
				node_stack[++top] = curr;
				stack_versions[top] = GET_VERSION(curr);
			}

			/* External node reached. */
			if (IS_EXTERNAL_NODE(curr)) {
				if (curr->key != key) {
					__builtin_tend(0);
					#ifdef USE_CPU_LOCK
					/* FIXME free cpu_lock here? */
					if (cpu_locks[my_cpu_lock].owner == tid)
						pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
					#endif
					return 0;
				}
				__builtin_tend(0);
				break;
			}

			__builtin_tend(0);
			continue;
		} else {
			tdata->tx_aborts++;
			tdata->tx_stats[2][1][1]++;
			texasru_t texasru = __builtin_get_texasru();
			if (_TEXASRU_ABORT(texasru) && 
			    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
				tdata->tx_stats[2][1][5]++;
				tdata->tx_aborts_version_error++;
				goto try_from_scratch;
			} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
				tdata->tx_stats[2][1][4]++;
				tdata->tx_aborts_footprint_overflow++;
			} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
				tdata->tx_stats[2][1][2]++;
				tdata->tx_aborts_transaction_conflict++;
			} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
				tdata->tx_stats[2][1][3]++;
				tdata->tx_aborts_non_transaction_conflict++;
			} else {
				tdata->tx_stats[2][1][6]++;
				tdata->tx_aborts_rest++;
			}
			goto retry_window;
		}
	}

	delete_fixup_retries = -1;
retry_deletion:
	delete_fixup_retries++;
//	if (delete_fixup_retries >= 2 * TX_NUM_RETRIES) {
	if (delete_fixup_retries >= TX_NUM_RETRIES) {
		tdata->tx_stats[2][2][7]++;
		goto try_from_scratch;
	}

#	ifdef USE_CPU_LOCK
	while (cpu_locks[my_cpu_lock].owner > 0 &&
	       cpu_locks[my_cpu_lock].owner != tid &&
	       cpu_locks[my_cpu_lock].spinlock == 1)
		;

	if (delete_fixup_retries >= TX_NUM_RETRIES &&
	    cpu_locks[my_cpu_lock].owner < 0) {
		pthread_spin_lock(&cpu_locks[my_cpu_lock].spinlock);
		cpu_locks[my_cpu_lock].owner = tid;
	}
#	endif

	/* Last transaction to delete the node and fixup. */
	while (rbt->spinlock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[2][2][0]++;
	if (__builtin_tbegin(0)) {
#		ifdef USE_CPU_LOCK
		if (cpu_locks[my_cpu_lock].owner > 0 &&
		    cpu_locks[my_cpu_lock].owner != tid &&
		    cpu_locks[my_cpu_lock].spinlock == 1)
			__builtin_tabort(0x77);
#		endif

		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
		/* Check that window version is unchanged. */
		if (stack_versions[top] != GET_VERSION(curr))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		/* Delete the node and fixup any violations. */
		parent = (top >= 1) ? node_stack[top-1] : NULL;
		gparent = (top >= 2) ? node_stack[top-2] : NULL;

		if (!parent) { /* Only one node in the tree. */
			nodes_to_delete[0] = curr;
			nodes_to_delete[1] = NULL;
			rbt->root = NULL;
			INC_VERSION(curr);
			rbt->version++;
			__builtin_tend(0);
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner == tid) {
				cpu_locks[my_cpu_lock].owner = -1;
				pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
			}
#			endif
			return 1; /* No fixup necessary. */
		} else if (!gparent) { /* We don't have gparent so parent is the root. */
			if (stack_versions[top-1] != GET_VERSION(parent))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			int dir_from_parent = parent->key < key;
	
			rbt->root = parent->link[!dir_from_parent];
			nodes_to_delete[0] = parent;
			nodes_to_delete[1] = curr;
			INC_VERSION(parent);
			INC_VERSION(curr);
			INC_VERSION(parent->link[!dir_from_parent]);
			rbt->version++;
			__builtin_tend(0);
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner == tid) {
				cpu_locks[my_cpu_lock].owner = -1;
				pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
			}
#			endif
			return 1; /* No fixup necessary. */
		} else {
			if (stack_versions[top-1] != GET_VERSION(parent) ||
			    stack_versions[top-2] != GET_VERSION(gparent))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			INC_VERSION(curr);
			INC_VERSION(parent);
			INC_VERSION(gparent);

			/* Refresh the version of the gparent (curr, and parent are 
			   going to be removed). */
			stack_versions[top-2] = GET_VERSION(gparent);

			int dir_from_gparent = gparent->key < key;
			int dir_from_parent = parent->key < key;
	
			gparent->link[dir_from_gparent] = parent->link[!dir_from_parent];
			nodes_to_delete[0] = parent;
			nodes_to_delete[1] = curr;
	
			/* If parent was BLACK we need to fixup.
			   First remove parent, curr from the stack and add curr's sibling.
			 */
			top--;
			node_stack[top] = parent->link[!dir_from_parent];
			stack_versions[top] = GET_VERSION(node_stack[top]);
	
			if (IS_BLACK(parent))
				_rbt_delete_fixup(rbt, key, node_stack, stack_versions, top);

			__builtin_tend(0);
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner == tid) {
				cpu_locks[my_cpu_lock].owner = -1;
				pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
			}
#			endif
			return 1;
		}

		/* Unreachable */
		assert(0);
	} else {
		tdata->tx_aborts++;
		tdata->tx_stats[2][2][1]++;
		texasru_t texasru = __builtin_get_texasru();
		if (_TEXASRU_ABORT(texasru) && 
		    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
			tdata->tx_stats[2][2][5]++;
			tdata->tx_aborts_version_error++;
			goto try_from_scratch;
		} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
			tdata->tx_stats[2][2][4]++;
			tdata->tx_aborts_footprint_overflow++;
		} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
			tdata->tx_stats[2][2][2]++;
			tdata->tx_aborts_transaction_conflict++;
		} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
			tdata->tx_stats[2][2][3]++;
			tdata->tx_aborts_non_transaction_conflict++;
		} else {
			tdata->tx_stats[2][2][6]++;
			tdata->tx_aborts_rest++;
		}
		goto retry_deletion;
	}

	/* Unreachable */
	assert(0);
	return 1;
}

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

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(rbt_node_t));
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
	return;
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret;
	ret = _rbt_lookup_helper(rbt, thread_data, key);
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
	rbt_node_t *nodes_to_delete[2] = {NULL, NULL};

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
	sprintf(str, "links_bu_external_fg_htm ( TRAVERSAL_TX_PATH_SIZE: %d )",
	        TRAVERSAL_TX_PATH_SIZE);
	return str;
}
