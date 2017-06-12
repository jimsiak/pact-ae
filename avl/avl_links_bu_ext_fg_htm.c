#include <htmintrin.h> /* power8 tm gcc intrinsics. */
#include <assert.h>
#include <pthread.h> //> pthread_spinlock_t

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

#define TX_STATS_ARRAY_NR_TRANS 3
#include "avl_links_bu_ext_fg_htm_thread_data.h"

#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )
#define GET_VERSION(node) ( (!(node)) ? 0 : (node)->version )
#define INC_VERSION(node) (node)->version++

#define IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN 0xfe
#define IMPLICIT_ABORT_VERSION_ERROR 0xee

#define MAX_ACCESS_PATH_LEN 40

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )

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

#define USE_VERSIONING
#include "avl_types.h"
#include "avl_utils.h"
#include "avl_warmup.h"
#include "avl_validate.h"

static int _avl_lookup_helper_serial(avl_t *avl, int key)
{
	int ret = 0;
	avl_node_t *curr = avl->root;

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

static int _avl_lookup_helper(avl_t *avl, htm_fg_tdata_t *tdata, int key)
{
	int dir, found;
	avl_node_t *curr;
	unsigned long long window_versions[1]; /* curr version */
	int retries = -1;
	int tx1_retries, tx2_retries;

try_from_scratch:

	retries++;

	/* Global lock fallback. */
	if (retries >= TX_NUM_RETRIES) {
		tdata->tx_lacqs++;
		int ret = 0;
		pthread_spin_lock(&avl->global_lock);
		ret = _avl_lookup_helper_serial(avl, key);
		pthread_spin_unlock(&avl->global_lock);
		return ret;
	}

	tx1_retries = -1;
TX1:
	tx1_retries++;
	if (tx1_retries >= TX_NUM_RETRIES)
		goto try_from_scratch;

	/* Avoid Lemming effect. */
	while (avl->global_lock == 1)
		;

	/* First transaction at the root. */
	tdata->tx_starts++;
	tdata->tx_stats[0][0][0]++;
	if (__builtin_tbegin(0)) {
		if (avl->global_lock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

		/* Empty tree. */
		if (!avl->root) {
			__builtin_tend(0);
			return 0;
		}

		curr = avl->root;
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
		while (avl->global_lock == 1)
			;

		tdata->tx_starts++;
		tdata->tx_stats[0][1][0]++;
		if (__builtin_tbegin(0)) {
			if (avl->global_lock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			if (window_versions[0] != GET_VERSION(curr))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

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

/*
 * 'top' shows at the last occupied index of 'node_stack' which is the newly
 * added node to the tree.
 */
static inline void _avl_insert_fixup(avl_t *avl, int key,
                                     avl_node_t *node_stack[MAX_ACCESS_PATH_LEN],
                                     unsigned long long stack_versions[MAX_ACCESS_PATH_LEN],
                                     int top, htm_fg_tdata_t *tdata)
{
	int lheight = -1, rheight = -1;
	avl_node_t *curr = NULL, *parent = NULL;
	int rotations = 0;

	top--; /* Ignore the newly added internal node. */
	while (top > 0) {
		curr = node_stack[top--];
		parent = node_stack[top];
		if (stack_versions[top+1] != GET_VERSION(curr) ||
		    stack_versions[top] != GET_VERSION(parent))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
		/* DEBUG: Don't yet know why but this is necessary here :( */
		int i;
		for (i=top-1; i>= top-3 && i >= 0; i--)
//		for (i=top-1; i >= 0; i--)
			if (stack_versions[i] != GET_VERSION(node_stack[i]))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		lheight = curr->link[0]->height;
		rheight = curr->link[1]->height;

		int balance = node_balance(curr);

		if (balance == 2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[0]);

			if (balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 1);
				rotations += 1;
			} else if (balance2 == -1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 1);
				rotations += 2;
			} else {
//				printf("Key = %d\n", key);
				assert(0);
			}

			INC_VERSION(parent);
			if (top == 0) {
				avl->root = parent->link[dir_from_parent];
				avl->version++;
			}

			break;
		} else if (balance == -2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[1]);

			if (balance2 == -1) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 0);
				rotations += 1;
			} else if (balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 0);
				rotations += 2;
			} else {
//				printf("Key = %d\n", key);
				assert(0);
			}

			INC_VERSION(parent);
			if (top == 0) {
				avl->root = parent->link[dir_from_parent];
				avl->version++;
			}

			break;
		}

		/* Update the height of curr. */
		int height_saved = curr->height;
		int height_new = MAX(lheight, rheight) + 1;
		curr->height = height_new;
		INC_VERSION(curr);
		if (height_saved == height_new)
			break;
	}

#	ifdef VERBOSE_STATISTICS
	tdata->restructures_at_level[top-1] += rotations;
#	endif
}

static inline int _avl_insert_helper_serial(avl_t *avl, avl_node_t *nodes[2],
                                            htm_fg_tdata_t *tdata)
{
	avl_node_t head = { 0 };
	avl_node_t *node_stack[MAX_ACCESS_PATH_LEN];
	unsigned long long stack_versions[MAX_ACCESS_PATH_LEN];
	avl_node_t *curr = NULL;
	int top = -1;

	/* Empty tree */
	if (!avl->root) {
		avl->root = nodes[0];
		avl->version++;
		return 1;
	}

	head.height = 0;
	head.link[1] = avl->root;
	node_stack[++top] = &head;
	stack_versions[top] = 0;

	/* Traverse the tree until an external node is reached. */
	curr = avl->root;
	node_stack[++top] = curr;
	stack_versions[top] = GET_VERSION(curr);
	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < nodes[0]->key;
		curr = curr->link[dir];
		node_stack[++top] = curr;
		stack_versions[top] = GET_VERSION(curr);
	}

	/* Did we find the external node we were looking for? */
	if (curr->key == nodes[0]->key)
		return 0;

	/* Insert the new node and fixup any violations. */
	replace_external_node(curr, nodes);
	INC_VERSION(curr);
	_avl_insert_fixup(avl, nodes[0]->key, node_stack, stack_versions, top, tdata);

	return 1;
}

static inline int _avl_insert_helper(avl_t *avl, avl_node_t *nodes[2],
                                     htm_fg_tdata_t *tdata)
{
	int i;
	avl_node_t head = { 0 };
	avl_node_t *node_stack[MAX_ACCESS_PATH_LEN];
	unsigned long long stack_versions[MAX_ACCESS_PATH_LEN];
	avl_node_t *curr = NULL;
	int top = -1;
	int retries = -1, tx1_retries = -1, tx2_retries = -1, tx3_retries = -1;

	unsigned long long tree_version;

try_from_scratch:

	retries++;
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
		pthread_spin_lock(&avl->global_lock);
		ret = _avl_insert_helper_serial(avl, nodes, tdata);
		pthread_spin_unlock(&avl->global_lock);
		return ret;
	}

	tx1_retries = -1;
TX1:
	tx1_retries++;
	if (tx1_retries >= TX_NUM_RETRIES)
		goto try_from_scratch;

	top = -1;

	/* Avoid Lemming effect. */
	while (avl->global_lock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[1][0][0]++;
	if (__builtin_tbegin(0)) {
		/* Empty tree */
		if (!avl->root) {
			avl->root = nodes[0];
			avl->version++;
			__builtin_tend(0);
			return 1;
		}
	
		head.height = 0;
		head.link[1] = avl->root;
		node_stack[++top] = &head;
		stack_versions[top] = 0;

		curr = avl->root;
		node_stack[++top] = curr;
		stack_versions[top] = GET_VERSION(curr);

		tree_version = avl->version;

		__builtin_tend(0);
	} else {
		ABORT_HANDLER(tdata, 1, 0);
		goto TX1;
	}

	/* Window transactions until an external node is reached. */
	while (1) {
		tx2_retries = -1;
TX2:
		tx2_retries++;
		if (tx2_retries >= TX_NUM_RETRIES) {
			tdata->tx_stats[1][1][7]++;
			goto try_from_scratch;
		}

		while (avl->global_lock == 1)
			;

		tdata->tx_starts++;
		tdata->tx_stats[1][1][0]++;
		if (__builtin_tbegin(0)) {
			if (avl->global_lock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			/* Check that window version is unchanged. */
			if (stack_versions[top] != GET_VERSION(curr))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
			if (tree_version != avl->version)
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
					return 0;
				}
				__builtin_tend(0);
				break;
			}

			__builtin_tend(0);
			continue;
		} else {
			ABORT_HANDLER(tdata,1,1);
			goto TX2;
		}
	}

	tx3_retries = -1;
TX3:
	tx3_retries++;
	if (tx3_retries >= TX_NUM_RETRIES) {
		tdata->tx_stats[1][2][7]++;
		goto try_from_scratch;
	}

	while (avl->global_lock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[1][2][0]++;
	if (__builtin_tbegin(0)) {
		/* DEBUG */
//		int i;
//		for (i=top; i >= top-5; i--)
//			if (stack_versions[i] != GET_VERSION(node_stack[i]))
//				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		if (avl->global_lock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
		/* Check that window version is unchanged. */
		if (stack_versions[top] != GET_VERSION(curr))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
		if (tree_version != avl->version)
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		/* Insert the new node and fixup any violations. */
		replace_external_node(curr, nodes);
		INC_VERSION(curr);
		_avl_insert_fixup(avl, nodes[0]->key, node_stack, stack_versions, top, tdata);

		__builtin_tend(0);
	} else {
		ABORT_HANDLER(tdata,1,2);
		goto TX3;
	}

	return 1;
}

/*
 * The bottom of the stack contains the first node that might be unbalanced.
 */
static inline void _avl_delete_fixup(avl_t *avl, int key,
                                     avl_node_t *node_stack[MAX_ACCESS_PATH_LEN],
                                     unsigned long long stack_versions[MAX_ACCESS_PATH_LEN],
                                     int top, htm_fg_tdata_t *tdata)
{
	int lheight = -1, rheight = -1;
	avl_node_t *curr = NULL, *parent = NULL;
	int rotations = 0;

	while (top > 0) {
		curr = node_stack[top--];
		parent = node_stack[top];
		if (stack_versions[top+1] != GET_VERSION(curr) ||
		    stack_versions[top] != GET_VERSION(parent))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		lheight = curr->link[0]->height;
		rheight = curr->link[1]->height;

		int balance = node_balance(curr);

		if (balance == 2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[0]);

			if (balance2 == 0 || balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 1);
				rotations += 1;
			} else if (balance2 == -1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 1);
				rotations += 2;
			} else {
				assert(0);
			}

			INC_VERSION(parent);
			stack_versions[top] = GET_VERSION(parent);
			if (top == 0) {
				avl->root = parent->link[dir_from_parent];
				avl->version++;
			}

			continue;
		} else if (balance == -2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[1]);

			if (balance2 == -1 || balance2 == 0) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 0);
				rotations += 1;
			} else if (balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 0);
				rotations += 2;
			} else {
				assert(0);
			}

			INC_VERSION(parent);
			stack_versions[top] = GET_VERSION(parent);
			if (top == 0) {
				avl->root = parent->link[dir_from_parent];
				avl->version++;
			}

			continue;
		}

		/* Update the height of curr. */
		int height_saved = curr->height;
		int height_new = MAX(lheight, rheight) + 1;
		curr->height = height_new;
		INC_VERSION(curr);
		if (height_saved == height_new)
			break;
	}

#	ifdef VERBOSE_STATISTICS
	tdata->restructures_at_level[top-1] += rotations;
#	endif
}

static inline int _avl_delete_helper_serial(avl_t *avl, int key,
                                            avl_node_t *nodes_to_delete[2],
                                            htm_fg_tdata_t *tdata)
{
	avl_node_t head = { 0 };
	avl_node_t *node_stack[MAX_ACCESS_PATH_LEN];
	unsigned long long stack_versions[MAX_ACCESS_PATH_LEN];
	avl_node_t *curr = NULL;
	int top = -1;

	/* Empty tree */
	if (!avl->root)
		return 0;

	head.link[1] = avl->root;
	node_stack[++top] = &head;
	stack_versions[top] = 0;

	/* Traverse the tree until an external node is reached. */
	curr = avl->root;
	node_stack[++top] = curr;
	stack_versions[top] = GET_VERSION(curr);
	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < key;
		curr = curr->link[dir];
		node_stack[++top] = curr;
		stack_versions[top] = GET_VERSION(curr);
	}

	/* Did we find the external node we were looking for? */
	if (curr->key != key)
		return 0;

	/* Unlink the leaf and fixup any violations. */
	avl_node_t *leaf = node_stack[top];
	if (top == 1) { /* Only one node in the tree. */
		nodes_to_delete[0] = leaf;
		nodes_to_delete[1] = NULL;
		avl->root = NULL;
		avl->version++;
	} else if (top == 2) { /* Leaf is root's child. */
		int dir = avl->root->key < key;
		nodes_to_delete[0] = avl->root;
		nodes_to_delete[1] = leaf;
		INC_VERSION(avl->root);
		INC_VERSION(avl->root->link[0]);
		INC_VERSION(avl->root->link[1]);
		avl->root = avl->root->link[!dir];
		avl->version++;
	} else {
		avl_node_t *parent = node_stack[top-1];
		avl_node_t *gparent = node_stack[top-2];
		int dir_gparent = gparent->key < key;
		int dir_parent = parent->key < key;
		nodes_to_delete[0] = parent;
		nodes_to_delete[1] = leaf;
		gparent->link[dir_gparent] = parent->link[!dir_parent];
		INC_VERSION(gparent);
		INC_VERSION(parent);
		INC_VERSION(parent->link[0]);
		INC_VERSION(parent->link[1]);
		top -= 2;
		stack_versions[top] = GET_VERSION(gparent);
		_avl_delete_fixup(avl, key, node_stack, stack_versions, top, tdata);
	}

	return 1;
}

static inline int _avl_delete_helper(avl_t *avl, int key,
                                     avl_node_t *nodes_to_delete[2],
                                     htm_fg_tdata_t *tdata)
{
	int i;
	avl_node_t head = { 0 };
	avl_node_t *node_stack[MAX_ACCESS_PATH_LEN];
	unsigned long long stack_versions[MAX_ACCESS_PATH_LEN];
	avl_node_t *curr = NULL;
	int top = -1;
	int retries = -1, tx1_retries = -1, tx2_retries = -1, tx3_retries = -1;

	unsigned long long tree_version;

try_from_scratch:
	
	retries++;
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
		pthread_spin_lock(&avl->global_lock);
		ret = _avl_delete_helper_serial(avl, key, nodes_to_delete, tdata);
		pthread_spin_unlock(&avl->global_lock);
		return ret;
	}

	tx1_retries = -1;
TX1:
	tx1_retries++;
	if (tx1_retries >= TX_NUM_RETRIES)
		goto try_from_scratch;

	top = -1;

	/* Avoid Lemming effect. */
	while (avl->global_lock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[2][0][0]++;
	if (__builtin_tbegin(0)) {
		/* Empty tree */
		if (!avl->root) {
			__builtin_tend(0);
			return 0;
		}
	
		head.height = 0;
		head.link[1] = avl->root;
		node_stack[++top] = &head;
		stack_versions[top] = 0;

		curr = avl->root;
		node_stack[++top] = curr;
		stack_versions[top] = GET_VERSION(curr);

		tree_version = avl->version;
		__builtin_tend(0);
	} else {
		ABORT_HANDLER(tdata, 2, 0);
		goto TX1;
	}

	/* Window transactions until an external node is reached. */
	while (1) {
		tx2_retries = -1;
TX2:
		tx2_retries++;
		if (tx2_retries >= TX_NUM_RETRIES) {
			tdata->tx_stats[2][1][7]++;
			goto try_from_scratch;
		}

		while (avl->global_lock == 1)
			;

		tdata->tx_starts++;
		tdata->tx_stats[2][1][0]++;
		if (__builtin_tbegin(0)) {
			if (avl->global_lock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			/* Check that window version is unchanged. */
			if (stack_versions[top] != GET_VERSION(curr))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
			if (tree_version != avl->version)
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

			/* Did we find the external node we were looking for? */
			if (IS_EXTERNAL_NODE(curr)) {
				if (curr->key != key) {
					__builtin_tend(0);
					return 0;
				}
				__builtin_tend(0);
				break;
			}

			__builtin_tend(0);
			continue;
		} else {
			ABORT_HANDLER(tdata,2,1);
			goto TX2;
		}
	}

	tx3_retries = -1;
TX3:
	tx3_retries++;
	if (tx3_retries >= TX_NUM_RETRIES) {
		tdata->tx_stats[2][2][7]++;
		goto try_from_scratch;
	}

	while (avl->global_lock == 1)
		;

	tdata->tx_starts++;
	tdata->tx_stats[2][2][0]++;
	if (__builtin_tbegin(0)) {
//		/* DEBUG */
//		int i;
//		for (i=0; i <= top; i++)
//			if (stack_versions[i] != GET_VERSION(node_stack[i]))
//				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		if (avl->global_lock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
		/* Check that window version is unchanged. */
		if (stack_versions[top] != GET_VERSION(curr))
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
		if (tree_version != avl->version)
			__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

		/* Unlink the leaf and fixup any violations. */
		avl_node_t *leaf = node_stack[top];
		if (top == 1) { /* Only one node in the tree. */
			nodes_to_delete[0] = leaf;
			nodes_to_delete[1] = NULL;
			avl->root = NULL;
			INC_VERSION(leaf);
			avl->version++;
		} else if (top == 2) { /* Leaf is root's child. */
			int dir = avl->root->key < key;
			nodes_to_delete[0] = avl->root;
			nodes_to_delete[1] = leaf;
			INC_VERSION(avl->root);
			INC_VERSION(avl->root->link[0]);
			INC_VERSION(avl->root->link[1]);
			avl->root = avl->root->link[!dir];
			avl->version++;
		} else {
			avl_node_t *parent = node_stack[top-1];
			avl_node_t *gparent = node_stack[top-2];
			if (stack_versions[top-1] != GET_VERSION(parent) ||
			    stack_versions[top-2] != GET_VERSION(gparent))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
			int dir_gparent = gparent->key < key;
			int dir_parent = parent->key < key;
			nodes_to_delete[0] = parent;
			nodes_to_delete[1] = leaf;
			gparent->link[dir_gparent] = parent->link[!dir_parent];
			INC_VERSION(gparent);
			INC_VERSION(gparent->link[0]);
			INC_VERSION(gparent->link[1]);
//			INC_VERSION(parent);
			INC_VERSION(parent->link[0]);
			INC_VERSION(parent->link[1]);
			top -= 2;
			stack_versions[top] = GET_VERSION(gparent);
			_avl_delete_fixup(avl, key, node_stack, stack_versions, top, tdata);
		}

		__builtin_tend(0);
	} else {
		ABORT_HANDLER(tdata,2,2);
		goto TX3;
	}

	return 1;
}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(avl_node_t));
	return _avl_new_helper();
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

int rbt_lookup(void *avl, void *thread_data, int key)
{
	int ret;
	ret = _avl_lookup_helper(avl, thread_data, key);
	return ret;
}

int rbt_insert(void *avl, void *thread_data, int key, void *value)
{
	int ret;
	avl_node_t *nodes[2];

	nodes[0] = avl_node_new(key, value);
	nodes[1] = avl_node_new(key, value);

	ret = _avl_insert_helper(avl, nodes, thread_data);

	if (!ret) {
		free(nodes[0]);
		free(nodes[1]);
	}

	return ret;
}

int rbt_delete(void *avl, void *thread_data, int key)
{
	int ret;
	avl_node_t *nodes_to_delete[2] = {NULL, NULL};

	ret = _avl_delete_helper(avl, key, nodes_to_delete, thread_data);

//	if (ret) {
//		free(nodes_to_delete[0]);
//		free(nodes_to_delete[1]);
//	}

	return ret;
}

int rbt_validate(void *avl)
{
	int ret;
	ret = _avl_validate_helper(((avl_t *)avl)->root);
	return ret;
}

int rbt_warmup(void *avl, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret;
	ret = _avl_warmup_helper((avl_t *)avl, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "avl_links_bu_external_fg_htm";
}

#include "avl_print.h"
int rbt_print(void *rbt)
{
	avl_print_struct(rbt);
	return 0;
}
