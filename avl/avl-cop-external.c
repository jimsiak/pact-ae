/**
 * An internal binary search tree.
 **/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h> //> pthread_spinlock_t

#include "alloc.h"
#include "arch.h"

typedef struct {
	int tid;
	long long unsigned tx_starts, tx_aborts,
	                   tx_aborts_validation, lacqs;
} tdata_t;

static inline tdata_t *tdata_new(int tid)
{
	tdata_t *ret;
	XMALLOC(ret, 1);
	ret->tid = tid;
	ret->tx_starts = 0;
	ret->tx_aborts = 0;
	ret->tx_aborts_validation = 0;
	ret->lacqs = 0;
	return ret;
}

static inline void tdata_print(tdata_t *tdata)
{
	printf("TID %3d: %llu %llu %llu ( %llu )\n", tdata->tid, tdata->tx_starts,
	                               tdata->tx_aborts, tdata->tx_aborts_validation,
	                               tdata->lacqs);
}

static inline void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->tx_aborts_validation = d1->tx_aborts_validation +
	                            d2->tx_aborts_validation;
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
#	define TX_TEST()      _xtest()
#	define _XABORT_EXPLICIT	(1 << 0)
#	define _XABORT_CODE(x)	(((x) >> 24) & 0xff)
#endif
/*****************/


#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )

typedef struct avl_node_s {
	int key;
	void *data;

	int height;
	int live;

	struct avl_node_s *parent,
	                  *right, *left;

	struct avl_node_s *prev, *succ;

	char padding[CACHE_LINE_SIZE - 3 * sizeof(int) - sizeof(void *) -
	             5 * sizeof(struct node_s *)];
} __attribute__((packed)) __attribute__((aligned(CACHE_LINE_SIZE))) avl_node_t;

typedef struct {
	avl_node_t *root;

	volatile pthread_spinlock_t avl_lock;
} avl_t;

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )

static avl_node_t *avl_node_new(int key, void *data)
{
	avl_node_t *node;

	XMALLOC(node, 1);
	node->key = key;
	node->data = data;
	node->height = 0; // new nodes have height 0 and NULL has height -1.
	node->live = 0;
	node->parent = NULL;
	node->right = node->left = NULL;
	node->prev = node->succ = NULL;
	return node;
}

static inline int node_height(avl_node_t *n)
{
	if (!n)
		return -1;

	return n->height;
}

static inline int node_balance(avl_node_t *n)
{
	if (!n || IS_EXTERNAL_NODE(n))
		return 0;

	return node_height(n->left) - node_height(n->right);
}

static avl_t *_avl_new_helper()
{
	avl_t *avl;

	XMALLOC(avl, 1);
	avl->root = NULL;

	pthread_spin_init(&avl->avl_lock, PTHREAD_PROCESS_SHARED);

	return avl;
}

static inline avl_node_t *rotate_right(avl_node_t *node)
{
	assert(node != NULL && node->left != NULL);

	avl_node_t *node_left = node->left;

	node->left = node->left->right;
	if (node->left) node->left->parent = node;

	node_left->right = node;
	node_left->parent = node->parent;
	node->parent = node_left;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_left->height = MAX(node_height(node_left->left), node_height(node_left->right)) + 1;
	return node_left;
}
static inline avl_node_t *rotate_left(avl_node_t *node)
{
	assert(node != NULL && node->right != NULL);

	avl_node_t *node_right = node->right;

	node->right = node->right->left;
	if (node->right) node->right->parent = node;

	node_right->left = node;
	node_right->parent = node->parent;
	node->parent = node_right;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_right->height = MAX(node_height(node_right->left), node_height(node_right->right)) + 1;
	return node_right;
}

/** Returns:
  *   - NULL if tree is empty.
  *   - a pointer to an external node that contains `key`.
  *   - a pointer to the external node where the traversal ended.
 **/
static inline avl_node_t *_traverse(avl_t *avl, int key)
{
	avl_node_t *curr = avl->root;

	if (!curr)
		return NULL;

	while (!IS_EXTERNAL_NODE(curr)) {
		if (key <= curr->key)
			curr = curr->left;
		else
			curr = curr->right;
	}

	return curr;
}

/* `operation`: 0 for lookup, 1 for insertion, 2 for deletion. */
static inline void _lookup_verify(avl_t *avl, avl_node_t *node, int key, int operation)
{
	if (!node && !avl->root)
		return;

	if (!node)
		TX_ABORT(ABORT_VALIDATION_FAILURE);

//	if (!node || !node->live)
//		TX_ABORT(ABORT_VALIDATION_FAILURE);

	if (!IS_EXTERNAL_NODE(node))
		TX_ABORT(ABORT_VALIDATION_FAILURE);

	if (node->key == key)
		return;

	if (operation == 1) {
		if (key < node->key) {
			avl_node_t *prev = node->prev;
			if (prev && key <= prev->key)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		} else {
			avl_node_t *succ = node->succ;
			if (succ && key >= succ->key)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
	}
//	} else if (operation == 2) {
//		node = node->parent;
//		if (node == avl->root && !IS_SENTINEL_NODE(avl->root->right))
//			TX_ABORT(ABORT_VALIDATION_FAILURE);
//
//		if (key < node->key) {
//			avl_node_t *prev = node->prev;
//			if (!IS_SENTINEL_NODE(prev) && key <= prev->key)
//				TX_ABORT(ABORT_VALIDATION_FAILURE);
//		} else {
//			avl_node_t *succ = node->succ;
//			if (!IS_SENTINEL_NODE(succ) && key >= succ->key)
//				TX_ABORT(ABORT_VALIDATION_FAILURE);
//		}
//	}
}

static int _avl_lookup_helper(avl_t *avl, int key, tdata_t *tdata)
{
	avl_node_t *leaf;
	tm_begin_ret_t status;
	int ret, retries = -1;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->avl_lock);
		leaf = _traverse(avl, key);
		ret = (leaf && leaf->key == key);
		pthread_spin_unlock(&avl->avl_lock);
		return ret;
	}

	ret = 0;

	/* Asynchronized traversal. */
	leaf = _traverse(avl, key);

	/* Transactional verification. */
	while (avl->avl_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->avl_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		/* _lookup_verify() will abort if verification fails. */
		_lookup_verify(avl, leaf, key, 0);
		ret = (leaf && leaf->key == key);

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	return ret;
}

static inline void _avl_insert_fixup(avl_t *avl, int key, avl_node_t *place)
{
	avl_node_t *curr, *parent;

	curr = place;
	parent = NULL;

	while (curr) {
		parent = curr->parent;

		int balance = node_balance(curr);
		if (balance == 2) {
			int balance2 = node_balance(curr->left);

			if (balance2 == 1) { // LEFT-LEFT case
				if (!parent)                avl->root = rotate_right(curr);
				else if (key < parent->key) parent->left = rotate_right(curr);
				else                        parent->right = rotate_right(curr);
			} else if (balance2 == -1) { // LEFT-RIGHT case
				curr->left = rotate_left(curr->left);
				if (!parent)                avl->root = rotate_right(curr); 
				else if (key < parent->key) parent->left = rotate_right(curr);
				else                        parent->right = rotate_right(curr);
			} else {
				assert(0);
			}

			break;
		} else if (balance == -2) {
			int balance2 = node_balance(curr->right);

			if (balance2 == -1) { // RIGHT-RIGHT case
				if (!parent)                avl->root = rotate_left(curr);
				else if (key < parent->key) parent->left = rotate_left(curr);
				else                        parent->right = rotate_left(curr);
			} else if (balance2 == 1) { // RIGHT-LEFT case
				curr->right = rotate_right(curr->right);
				if (!parent)                avl->root = rotate_left(curr);
				else if (key < parent->key) parent->left = rotate_left(curr);
				else                        parent->right = rotate_left(curr);
			} else {
				assert(0);
			}

			break;
		}

		/* Update the height of current node. */
		int height_saved = node_height(curr);
		int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
		curr->height = height_new;
		if (height_saved == height_new)
			break;

		curr = parent;
	}
}

/* `place` is an external node. */
static inline int _insert(avl_t *avl, avl_node_t *new_node[2], avl_node_t *place)
{
	avl_node_t *parent = NULL;
	avl_node_t *prev = NULL, *succ = NULL;

	// Empty tree.
	if (!place) {
		avl->root = new_node[0];
		return 1;
	}

	// Key already in the tree.
	if (place->key == new_node[0]->key)
		return 0;

	parent = place->parent;

	if (place->key < new_node[0]->key) {
		new_node[0]->key = place->key;
		new_node[0]->left = place;
		place->parent = new_node[0];
		new_node[0]->right = new_node[1];
		new_node[1]->parent = new_node[0];

		new_node[1]->succ = place->succ;
		new_node[1]->prev = place;
		place->succ = new_node[1];
	} else {
		new_node[0]->left = new_node[1];
		new_node[0]->right = place;

		new_node[1]->parent = new_node[0];
		place->parent = new_node[0];

		new_node[1]->prev = place->prev;
		new_node[1]->succ = place;
		place->prev = new_node[1];
	}

	new_node[0]->parent = parent;
	if (!parent) avl->root = new_node[0];
	else if (new_node[0]->key < parent->key) parent->left = new_node[0];
	else                                     parent->right = new_node[0];

	_avl_insert_fixup(avl, new_node[0]->key, new_node[0]);
	return 1;
}

static int _avl_insert_helper(avl_t *avl, int key, void *value, tdata_t *tdata)
{
	avl_node_t *leaf;
	tm_begin_ret_t status;
	int retries = -1, ret = 0;

	avl_node_t *new_node[2];
	new_node[0] = avl_node_new(key, NULL);
	new_node[1] = avl_node_new(key, value);

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->avl_lock);
		leaf = _traverse(avl, key);
		ret = _insert(avl, new_node, leaf);
		pthread_spin_unlock(&avl->avl_lock);
		if (!ret) {
			free(new_node[0]);
			free(new_node[1]);
		}
		return ret;
	}

	/* Asynchronized traversal. */
	leaf = _traverse(avl, key);

	/* Transactional verification. */
	while (avl->avl_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->avl_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN); 

		/* _lookup_verify() will abort if verification fails. */
		_lookup_verify(avl, leaf, key, 1);
		ret = _insert(avl, new_node, leaf);

		TX_END(0);
	} else {
		if ((status & _XABORT_EXPLICIT) && (_XABORT_CODE(status) == ABORT_VALIDATION_FAILURE))
			tdata->tx_aborts_validation++;
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	if (!ret) {
		free(new_node[0]);
		free(new_node[1]);
	}

	return ret;
}

static int _avl_insert_warmup_helper(avl_t *avl, int key, void *value)
{
	avl_node_t *leaf = NULL;
	avl_node_t *new_node[2];

	new_node[0] = avl_node_new(key, NULL);
	new_node[1] = avl_node_new(key, value);

	leaf = _traverse(avl, key);
	int ret = _insert(avl, new_node, leaf);
	if (!ret) {
		free(new_node[0]);
		free(new_node[1]);
	}
	return ret;
}

static inline void _avl_delete_fixup(avl_t *avl, int key, avl_node_t *place)
{
//	avl_node_t *curr, *parent;
//
//	curr = place;
//	parent = NULL;
//
//	while (!IS_SENTINEL_NODE(curr)) {
//		parent = curr->parent;
//
//		int balance = node_balance(curr);
//		if (balance == 2) {
//			int balance2 = node_balance(curr->left);
//
//			if (balance2 == 0 || balance2 == 1) { // LEFT-LEFT case
////				if (!parent)                avl->root = rotate_right(curr);
////				else if (key < parent->key) parent->left = rotate_right(curr);
//				if (key < parent->key) parent->left = rotate_right(curr);
//				else                   parent->right = rotate_right(curr);
//			} else if (balance2 == -1) { // LEFT-RIGHT case
//				curr->left = rotate_left(curr->left);
////				if (!parent)                avl->root = rotate_right(curr); 
////				else if (key < parent->key) parent->left = rotate_right(curr);
//				if (key < parent->key) parent->left = rotate_right(curr);
//				else                        parent->right = rotate_right(curr);
//			} else {
//				assert(0);
//			}
//
//			curr = parent;
//			continue;
//		} else if (balance == -2) {
//			int balance2 = node_balance(curr->right);
//
//			if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
////				if (!parent)                avl->root = rotate_left(curr);
////				else if (key < parent->key) parent->left = rotate_left(curr);
//				if (key < parent->key) parent->left = rotate_left(curr);
//				else                        parent->right = rotate_left(curr);
//			} else if (balance2 == 1) { // RIGHT-LEFT case
//				curr->right = rotate_right(curr->right);
////				if (!parent)                avl->root = rotate_left(curr);
////				else if (key < parent->key) parent->left = rotate_left(curr);
//				if (key < parent->key) parent->left = rotate_left(curr);
//				else                        parent->right = rotate_left(curr);
//			} else {
//				assert(0);
//			}
//
//			curr = parent;
//			continue;
//		}
//
//		/* Update the height of current node. */
//		int height_saved = node_height(curr);
//		int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
//		curr->height = height_new;
//		if (height_saved == height_new)
//			break;
//
//		curr = parent;
//	}
}

static avl_node_t *avl_minimum_node(avl_node_t *root)
{
	return NULL;
//	avl_node_t *ret;
//
//	if (IS_SENTINEL_NODE(root))
//		return root;
//
//	for (ret = root; !IS_SENTINEL_NODE(ret->left); ret = ret->left)
//		;
//
//	return ret;
}

static inline int _delete(avl_t *avl, int key, avl_node_t *z)
{
	return 0;
//	avl_node_t *x, *y;
//
//	if (z->key != key)
//		return 0;
//
//	/**
//	 * 2 cases for z:
//	 *   - zero or one child: z is to be removed.
//	 *   - two children: the leftmost node of z's 
//	 *     right subtree is to be removed.
//	 **/
//	if (IS_SENTINEL_NODE(z->left) || IS_SENTINEL_NODE(z->right))
//		y = z;
//	else
//		y = avl_minimum_node(z->right);
//
//	x = y->left;
//	if (IS_SENTINEL_NODE(x))
//		x = y->right;
//
//	/* replace y with x */
//	x->parent = y->parent;
//	if (y == y->parent->left)
//		y->parent->left = x;
//	else
//		y->parent->right = x;
//
//	if (y != z) {
//		z->key = y->key;
//		z->data = y->data;
//	}
//
//	avl_node_t *prev = y->prev;
//	avl_node_t *succ = y->succ;
//	if (prev) prev->succ = succ;
//	if (succ) succ->prev = prev;
//	y->live = 0;
//	if (IS_SENTINEL_NODE(y->left)) y->left->live = 0;
//	if (IS_SENTINEL_NODE(y->right)) y->right->live = 0;
//
//	_avl_delete_fixup(avl, y->key, x->parent);
//
//	return 1;
}

static int _avl_delete_helper(avl_t *avl, int key, tdata_t *tdata)
{
	avl_node_t *leaf;
	tm_begin_ret_t status;
	int retries = -1, ret = 0;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->avl_lock);
		leaf = _traverse(avl, key);
		ret = _delete(avl, key, leaf);
		pthread_spin_unlock(&avl->avl_lock);
		return ret;
	}

	/* Asynchronized traversal. */
	leaf = _traverse(avl, key);

	/* Transactional verification. */
	while (avl->avl_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->avl_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		/* _lookup_verify() will abort if verification fails. */
		_lookup_verify(avl, leaf, key, 2);
		ret = _delete(avl, key, leaf);

		TX_END(0);
	} else {
		if ((status & _XABORT_EXPLICIT) && (_XABORT_CODE(status) == ABORT_VALIDATION_FAILURE))
			tdata->tx_aborts_validation++;
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	return ret;
}

static inline int _avl_warmup_helper(avl_t *avl, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = _avl_insert_warmup_helper(avl, key, NULL);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

static int total_paths, total_nodes, bst_violations, avl_violations;
static int min_path_len, max_path_len;
static void _avl_validate_rec(avl_node_t *root, int _th)
{
	if (!root)
		return;

	avl_node_t *left = root->left;
	avl_node_t *right = root->right;

	total_nodes++;
	_th++;

	/* BST violation? */
	if (left && left->key > root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	if (IS_EXTERNAL_NODE(root)) {
		total_paths++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* AVL violation? */
	int balance = node_balance(root);
	if (balance < -1 || balance > 1)
		avl_violations++;

	/* Check subtrees. */
	_avl_validate_rec(left, _th);
	_avl_validate_rec(right, _th);
}

static inline int _avl_validate_helper(avl_node_t *root)
{
	int check_bst = 0, check_avl = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;
	avl_violations = 0;

	_avl_validate_rec(root, 0);

	check_bst = (bst_violations == 0);
	check_avl = (avl_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  AVL Violation: %s\n",
	       check_avl ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst && check_avl;
}

/*********************    FOR DEBUGGING ONLY    *******************************/
static void avl_print_rec(avl_node_t *root, int level)
{
	int i;

	if (root)
		avl_print_rec(root->right, level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%d (%d, %d)\n", root->key,
	       root->prev ? root->prev->key : -1,
	       root->succ ? root->succ->key : -1);

	avl_print_rec(root->left, level + 1);
}

static void avl_print_struct(avl_t *avl)
{
	if (!avl->root)
		printf("[empty]");
	else
		avl_print_rec(avl->root, 0);
	printf("\n");
}

int rbt_print(void *rbt)
{
	avl_print_struct(rbt);
	return 0;
}
/******************************************************************************/

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
	return tdata_new(tid);
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
	ret = _avl_lookup_helper(rbt, key, thread_data);
	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = _avl_insert_helper(rbt, key, value, thread_data);
	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	ret = _avl_delete_helper(rbt, key, thread_data);
	return ret;
}

int rbt_validate(void *rbt)
{
	int ret = 0;
	ret = _avl_validate_helper(((avl_t *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _avl_warmup_helper((avl_t *)rbt, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "avl-cop-external";
}
