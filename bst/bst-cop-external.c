#include <assert.h>
#include <pthread.h> //> pthread_spinlock_t

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

typedef struct {
	int tid;
	long long unsigned tx_starts, tx_aborts, lacqs;
} tdata_t;

static inline tdata_t *tdata_new(int tid)
{
	tdata_t *ret;
	XMALLOC(ret, 1);
	ret->tid = tid;
	ret->tx_starts = 0;
	ret->tx_aborts = 0;
	ret->lacqs = 0;
	return ret;
}

static inline void tdata_print(tdata_t *tdata)
{
	printf("TID %3d: %llu %llu ( %llu )\n", tdata->tid, tdata->tx_starts,
	                               tdata->tx_aborts, tdata->lacqs);
}

static inline void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->lacqs = d1->lacqs + d2->lacqs;
}

/* TM Interface. */
#if !defined(TX_NUM_RETRIES)
#	define TX_NUM_RETRIES 20
#endif

#ifdef __POWERPC64__
#	include <htmintrin.h>
	typedef int tm_begin_ret_t;
#	define LOCK_FREE 0
#	define TM_BEGIN_SUCCESS 1
#	define ABORT_VALIDATION_FAILURE 0xee
#	define ABORT_GL_TAKEN           0xff
#	define TX_ABORT(code) __builtin_tabort(code)
#	define TX_BEGIN(code) __builtin_tbegin(0)
#	define TX_END(code)   __builtin_tend(0)
#else
#	include "rtm.h"
	typedef unsigned tm_begin_ret_t;
#	define LOCK_FREE 1
#	define TM_BEGIN_SUCCESS _XBEGIN_STARTED
#	define ABORT_VALIDATION_FAILURE 0xee
#	define ABORT_GL_TAKEN           0xff
#	define TX_ABORT(code) _xabort(code)
#	define TX_BEGIN(code) _xbegin()
#	define TX_END(code)   _xend()
#endif
/*****************/

typedef enum {
	RED = 0,
	BLACK
} color_t;

typedef struct rbt_node {
	void *data;
	struct rbt_node *parent,
	                *left, *right,
	                *prev, *succ;

	int key;
	int live;

	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) -
	             sizeof(void *) - 5 * sizeof(struct rbt_node *)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;

typedef struct {
	rbt_node_t *root;

	pthread_spinlock_t rbt_lock;
} rbt_t;

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )

static rbt_node_t *rbt_node_new(int key, void *data)
{
	rbt_node_t *node;
	
	XMALLOC(node, 1);
	node->key = key;
	node->parent = NULL;
	node->left = NULL;
	node->right = NULL;
	node->prev = NULL;
	node->succ = NULL;
	node->data = data;
	node->live = 0;

	return node;
}

rbt_t *_rbt_new_helper()
{
	rbt_t *rbt;

	XMALLOC(rbt, 1);
	rbt->root = NULL;

	pthread_spin_init(&rbt->rbt_lock, PTHREAD_PROCESS_SHARED);

	return rbt;
}

static inline void _lookup_verify(rbt_node_t *node, int key)
{
	if (!node || !node->live)
		TX_ABORT(ABORT_VALIDATION_FAILURE);

	if (!IS_EXTERNAL_NODE(node))
		TX_ABORT(ABORT_VALIDATION_FAILURE);

	if (node->key == key)
		return;

	if (key < node->key) {
		rbt_node_t *prev = node->prev;
		if (prev && key <= prev->key) {
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	} else {
		rbt_node_t *succ = node->succ;
		if (succ && key >= succ->key) {
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	}
}

/** Returns a pointer to the leaf node that contains `key` or
 *  to the parent(leaf) of the node that would contain it.
 **/
static rbt_node_t *_traverse(rbt_t *rbt, int key)
{
	rbt_node_t *curr = rbt->root;

	if (!curr)
		return NULL;

	while (curr && !IS_EXTERNAL_NODE(curr)) {
		if (key <= curr->key)
			curr = curr->left;
		else if (key > curr->key)
			curr = curr->right;
	}

	return curr;
}


/**
 * Returns 1 if found, else 0.
 **/
int _rbt_lookup_helper(rbt_t *rbt, int key, tdata_t *tdata)
{
	tm_begin_ret_t status;
	rbt_node_t *place;
	int ret, retries = -1;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&rbt->rbt_lock);
		place = _traverse(rbt, key);
		ret = (place && place->key == key);
		pthread_spin_unlock(&rbt->rbt_lock);
		return ret;
	}

	ret = 0;

	/* Asynchronized traversal. */
	place = _traverse(rbt, key);

	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		/* _lookup_verify() will abort if verification fails. */
		_lookup_verify(place, key);
		ret = (place && place->key == key);

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	return ret;
}

/**
 * Replace an external node with an internal with two children.
 * Example;
 *
 *       8                4
 *     /   \     =>     /   \
 *   NULL NULL         4     8
 *                   /   \ /   \
 *                 NULL  NULL  NULL
 * Hint: Also updates `prev` and `succ` of each node.
 **/
static inline void replace_external_node(rbt_node_t *root,
                                         rbt_node_t *nodes[2])
{
	root->left = nodes[0];
	root->right = nodes[1];
	root->left->live = 1;
	root->right->live = 1;

	root->left->parent = root;
	root->right->parent = root;

	if (root->key > nodes[0]->key) {
		root->right->key = root->key;
		root->key = root->left->key;
	} else {
		root->left->key = root->key;
	}

	if (root->prev)
		root->prev->succ = root->left;
	if (root->succ)
		root->succ->prev = root->right;

	root->left->prev = root->prev;
	root->left->succ = root->right;
	root->right->prev = root->left;
	root->right->succ = root->succ;
	root->prev = NULL;
	root->succ = NULL;
}

/**
 *  Returns 0 if key was already there, 1 otherwise.
 **/
static inline int _insert(rbt_t *rbt, rbt_node_t *place, rbt_node_t *new_nodes[2])
{
	rbt_node_t *prev = NULL, *succ = NULL;

	/* Empty tree. */
	if (!place) {
		rbt->root = new_nodes[0];
		rbt->root->live = 1;
		free(new_nodes[1]);
		return 1;
	}

	if (place->key == new_nodes[0]->key)
		return 0;

	replace_external_node(place, new_nodes);

	return 1;
}

int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *new_nodes[2], tdata_t *tdata)
{
	tm_begin_ret_t status;
	rbt_node_t *place;
	int retries = -1, ret = 0;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&rbt->rbt_lock);
		place = _traverse(rbt, new_nodes[0]->key);
		ret = _insert(rbt, place, new_nodes);
		pthread_spin_unlock(&rbt->rbt_lock);
		return ret;
	}

	/* Asynchronized traversal. */
	place = _traverse(rbt, new_nodes[0]->key);

	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		/* _lookup_verify() will abort if verification fails. */
		_lookup_verify(place, new_nodes[0]->key);
		ret = _insert(rbt, place, new_nodes);

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	return ret;

}

int _rbt_insert_helper_warmup(rbt_t *rbt, rbt_node_t *new_nodes[2], tdata_t *tdata)
{
	rbt_node_t *place = _traverse(rbt, new_nodes[0]->key);
	return _insert(rbt, place, new_nodes);
}

static int _delete(rbt_t *rbt, int key, rbt_node_t *z, rbt_node_t *nodes_to_free[2])
{
	if (!z || z->key != key)
		return 0;

	if (z == rbt->root) {
		rbt->root = NULL;
		nodes_to_free[0] = z;
		nodes_to_free[1] = NULL;
		z->live = 0;
		return 1;
	}

	rbt_node_t *sibling;
	if (z == z->parent->left) {
		sibling = z->parent->right;
		if (z->parent == rbt->root) {
			rbt->root = sibling;
			sibling->parent = NULL;
			nodes_to_free[0] = z->parent;
			nodes_to_free[1] = z;
		} else {
			if (z->parent == z->parent->parent->left)
				z->parent->parent->left = sibling;
			else
				z->parent->parent->right = sibling;

			sibling->parent = z->parent->parent;
		}
		if (z->prev) z->prev->succ = z->succ;
		if (z->succ) z->succ->prev = z->prev;
		z->live = 0;
		z->parent->live = 0;
	} else {
		sibling = z->parent->left;
		if (z->parent == rbt->root) {
			rbt->root = sibling;
			sibling->parent = NULL;
			nodes_to_free[0] = z->parent;
			nodes_to_free[1] = z;
		} else {
			if (z->parent == z->parent->parent->left)
				z->parent->parent->left = sibling;
			else
				z->parent->parent->right = sibling;

			sibling->parent = z->parent->parent;
		}
		if (z->prev) z->prev->succ = z->succ;
		if (z->succ) z->succ->prev = z->prev;
		z->live = 0;
		z->parent->live = 0;
	}

	return 1;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t *nodes_to_free[2],
                              tdata_t *tdata)
{
	tm_begin_ret_t status;
	rbt_node_t *place;
	int retries = -1, ret = 0;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&rbt->rbt_lock);
		place = _traverse(rbt, key);
		ret = _delete(rbt, key, place, nodes_to_free);
		pthread_spin_unlock(&rbt->rbt_lock);
		return ret;
	}

	/* Asynchronized traversal. */
	place = _traverse(rbt, key);

	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		/* _lookup_verify() will abort if verification fails. */
		_lookup_verify(place, key);
		ret = _delete(rbt, key, place, nodes_to_free);

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	return ret;

}

static int bh;
static int paths_with_bh_diff;
static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes;
static int bst_violations;
static void _rbt_validate(rbt_node_t *root, int _th)
{
	if (!root)
		return;

	rbt_node_t *left = root->left;
	rbt_node_t *right = root->right;

	total_nodes++;
	_th++;

	/* BST violation? */
	if (left && left->key > root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* We found a path (an external node). */
	if (IS_EXTERNAL_NODE(root)) {
		total_paths++;
		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_rbt_validate(left, _th);
	if (right)
		_rbt_validate(right, _th);
}

static inline int _rbt_validate_helper(rbt_node_t *root)
{
	int check_bst = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;

	_rbt_validate(root, 0);

	check_bst = (bst_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size (Internal / Leaves / Total): %8d / %8d / %8d\n",
	       total_nodes - total_paths, total_paths, total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst;
}

static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	rbt_node_t *new_nodes[2];
	tdata_t *tdata = tdata_new(-1);
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		new_nodes[0] = rbt_node_new(key, NULL);
		new_nodes[1] = rbt_node_new(key, NULL);

		ret = _rbt_insert_helper_warmup(rbt, new_nodes, tdata);
		nodes_inserted += ret;

		if (!ret) {
			free(new_nodes[0]);
			free(new_nodes[1]);
		}
	}

	free(tdata);
	return nodes_inserted;
}

/*  */
/*********************    FOR DEBUGGING ONLY    *******************************/
static void rbt_print_rec(rbt_node_t *root, int level)
{
	int i;

	if (root)
		rbt_print_rec(root->right, level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%d\n", root->key);

	rbt_print_rec(root->left, level + 1);
}

static void rbt_print_struct(rbt_t *rbt)
{
	if (rbt->root == NULL)
		printf("[empty]");
	else
		rbt_print_rec(rbt->root, 0);
	printf("\n");
}

int rbt_print(void *rbt)
{
	rbt_print_struct(rbt);
	return 0;
}
/******************************************************************************/

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
	tdata_t *tdata = tdata_new(tid);

	return tdata;
}

void rbt_thread_data_print(void *thread_data)
{
	tdata_t *tdata = thread_data;
	tdata_print(tdata);
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
	tdata_add(d1, d2, dst);
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	tdata_t *tdata = thread_data;

	ret = _rbt_lookup_helper(rbt, key, tdata);

	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret;
	rbt_node_t *new_nodes[2];
	tdata_t *tdata = thread_data;

	new_nodes[0] = rbt_node_new(key, value);
	new_nodes[1] = rbt_node_new(key, value);

	ret = _rbt_insert_helper(rbt, new_nodes, tdata);

	if (!ret) {
		free(new_nodes[0]);
		free(new_nodes[1]);
	}

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret;
	rbt_node_t *node_to_free = NULL;
	tdata_t *tdata = thread_data;

	ret = _rbt_delete_helper(rbt, key, &node_to_free, tdata);

//	if (ret) {
////		if (IS_SENTINEL_NODE(node_to_free->left))
////			free(node_to_free->left);
////		if (IS_SENTINEL_NODE(node_to_free->right))
////			free(node_to_free->right);
//		free(node_to_free);
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
	return "links_bu_cop_external";
}
