#include <assert.h>
#include <pthread.h> //> pthread_spinlock_t
#include <string.h>  //> memset() for per thread allocator

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

/******************************************************************************/
/* A simple hash table implementation.                                        */
/******************************************************************************/
#define HT_LEN 16
#define HT_MAX_BUCKET_LEN 64
#define HT_GET_BUCKET(key) ((((long long)(key)) >> 4) % HT_LEN)
typedef struct {
	unsigned short bucket_next_index[HT_LEN];
	// Even numbers (0,2,4) are keys, odd numbers are values.
	void *entries[HT_LEN][HT_MAX_BUCKET_LEN * 2];
} ht_t;

ht_t *ht_new()
{
	int i;
	ht_t *ret;

	XMALLOC(ret, 1);
	memset(&ret->bucket_next_index[0], 0, sizeof(ret->bucket_next_index));
	memset(&ret->entries[0][0], 0, sizeof(ret->entries));
	return ret;
}

void ht_reset(ht_t *ht)
{
	memset(&ht->bucket_next_index[0], 0, sizeof(ht->bucket_next_index));
}

void ht_insert(ht_t *ht, void *key, void *value)
{
	int bucket = HT_GET_BUCKET(key);
	unsigned short bucket_index = ht->bucket_next_index[bucket];

	ht->bucket_next_index[bucket] += 2;

	assert(bucket_index < HT_MAX_BUCKET_LEN * 2);

	ht->entries[bucket][bucket_index] = key;
	ht->entries[bucket][bucket_index+1] = value;
}

void *ht_get(ht_t *ht, void *key)
{
	int bucket = HT_GET_BUCKET(key);
	int i;

	for (i=0; i < ht->bucket_next_index[bucket]; i+=2)
		if (key == ht->entries[bucket][i])
			return ht->entries[bucket][i+1];

//	assert(0);
	return NULL;
}

void ht_print(ht_t *ht)
{
	int i, j;

	for (i=0; i < HT_LEN; i++) {
		printf("BUCKET[%3d]:", i);
		for (j=0; j < ht->bucket_next_index[i]; j+=2)
			printf(" (%p, %p)", ht->entries[i][j], ht->entries[i][j+1]);
		printf("\n");
	}
}
/******************************************************************************/

typedef struct {
	int tid;
	long long unsigned tx_starts, tx_aborts, 
	                   tx_aborts_explicit_validation, lacqs;
	unsigned int next_node_to_allocate;
	ht_t *ht;
} tdata_t;

static inline tdata_t *tdata_new(int tid)
{
	tdata_t *ret;
	XMALLOC(ret, 1);
	ret->tid = tid;
	ret->tx_starts = 0;
	ret->tx_aborts = 0;
	ret->lacqs = 0;
	ret->next_node_to_allocate = 0;
	ret->ht = ht_new();
	return ret;
}

static inline void tdata_print(tdata_t *tdata)
{
	printf("TID %3d: %llu %llu %llu ( %llu )\n", tdata->tid, tdata->tx_starts,
	      tdata->tx_aborts, tdata->tx_aborts_explicit_validation, tdata->lacqs);
}

static inline void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->tx_aborts_explicit_validation = d1->tx_aborts_explicit_validation +
	                                     d2->tx_aborts_explicit_validation;
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
#	define ABORT_IS_CONFLICT(status) ((status) & _XABORT_CONFLICT)
#	define ABORT_IS_EXPLICIT(status) ((status) & _XABORT_EXPLICIT)
#	define ABORT_CODE(status) _XABORT_CODE(status)
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
	color_t color;
	int key;
	void *data;
	struct rbt_node *parent,
	                *left, *right;

//	char padding[CACHE_LINE_SIZE - sizeof(color_t) - sizeof(int) -
//	             sizeof(void *) - 3 * sizeof(struct rbt_node *)];
//} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;
} rbt_node_t;

typedef struct {
	rbt_node_t *root;

	pthread_spinlock_t rbt_lock;
} rbt_t;

unsigned int next_node_to_allocate;
rbt_node_t *per_thread_node_allocators[56];

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )
#define IS_BLACK(node) ( !(node) || (node)->color == BLACK )
#define IS_RED(node) ( !IS_BLACK(node) )

static rbt_node_t *rbt_node_new(int key, color_t color, void *data)
{
	rbt_node_t *node;
	
	XMALLOC(node, 1);
	node->color = color;
	node->key = key;
	node->data = data;
	node->parent = NULL;
	node->left = NULL;
	node->right = NULL;

	return node;
}

static void rbt_node_copy(rbt_node_t *dest, rbt_node_t *src)
{
//	memcpy(dest, src, sizeof(rbt_node_t));
	dest->color = src->color;
	dest->key = src->key;
	dest->parent = src->parent;
	dest->left = src->left;
	dest->right = src->right;
	__sync_synchronize();
}

static rbt_node_t *rbt_node_new_copy(rbt_node_t *src, tdata_t *tdata)
{
//	rbt_node_t *node = rbt_node_new(0, BLACK, NULL);
	rbt_node_t *node = &per_thread_node_allocators[tdata->tid][tdata->next_node_to_allocate++];
	rbt_node_copy(node, src);
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

static void rbt_rotate_left(rbt_t *rbt, rbt_node_t *x)
{
	rbt_node_t *y = x->right;
	
	x->right = y->left;
	y->left->parent = x;

	y->parent = x->parent;
	if (x->parent == NULL)
		rbt->root = y;
	else if (x == x->parent->left)
		x->parent->left = y;
	else
		x->parent->right = y;

	y->left = x;
	x->parent = y;
}

static void rbt_rotate_right(rbt_t *rbt, rbt_node_t *y)
{
	rbt_node_t *x = y->left;

	y->left = x->right;
	x->right->parent = y;

	x->parent = y->parent;
	if (y->parent == NULL)
		rbt->root = x;
	else if (y == y->parent->right)
		y->parent->right = x;
	else
		y->parent->left = x;
	
	x->right = y;
	y->parent = x;
}

/** Returns a pointer to the leaf node that contains `key` or
 *  to the parent(leaf) of the node that would contain it.
 **/
static inline rbt_node_t *_traverse(rbt_t *rbt, int key)
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

/** Returns a pointer to the leaf node that contains `key` or
 *  to the parent(leaf) of the node that would contain it.
 *  Also fills up the stack that contains the nodes in the access
 *  path.
 **/
static rbt_node_t *_traverse_with_stack(rbt_t *rbt, int key,
                                        rbt_node_t **node_stack, 
                                        int *stack_top)
{
	rbt_node_t *curr = rbt->root;

	*stack_top = -1;
	if (!curr)
		return NULL;

	while (curr && !IS_EXTERNAL_NODE(curr)) {
		node_stack[++(*stack_top)] = curr;
		if (key <= curr->key) {
			curr = curr->left;
		} else { // (key > curr->key)
			curr = curr->right;
		}
	}
	node_stack[++(*stack_top)] = curr;

	return curr;
}

/**
 * Returns 1 if found, else 0.
 **/
int _rbt_lookup_helper(rbt_t *rbt, int key, tdata_t *tdata)
{
	rbt_node_t *place;
	int ret;

	place = _traverse(rbt, key);
	ret = (place && place->key == key);
	return ret;
}

/**
 * z is the newly inserted internal node.
 **/
static void rbt_insert_fixup(rbt_t *rbt, rbt_node_t *z)
{
	while (IS_RED(z->parent)) {
		if (z->parent == z->parent->parent->left) { /* z->parent is left child */
			rbt_node_t *y = z->parent->parent->right; /* y = uncle */
			if (IS_RED(y)) { /* Case 1 */
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;
				z = z->parent->parent;
			} else {
				if (z == z->parent->right) { /* Case 2 */
					z = z->parent;
					rbt_rotate_left(rbt, z);
				}
				z->parent->color = BLACK; /* Case 3 */
				z->parent->parent->color = RED;
				rbt_rotate_right(rbt, z->parent->parent);
			} 
		} else { /* z->parent is right child */
			rbt_node_t *y = z->parent->parent->left; /* y = uncle */
			if (y->color == RED) { /* Case 1 */
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;
				z = z->parent->parent;
			} else {
				if (z == z->parent->left) { /* Case 2 */
					z = z->parent;
					rbt_rotate_right(rbt, z);
				}
				z->parent->color = BLACK; /* Case 3 */
				z->parent->parent->color = RED;
				rbt_rotate_left(rbt, z->parent->parent);
			}
		}
	}

	/* Recolor root if we colored it red. */
	if (IS_RED(rbt->root))
		rbt->root->color = BLACK;
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
 * Hint: Also updates `prev` and `succ` of each node.
 **/
static inline void replace_external_node(rbt_node_t *root,
                                         rbt_node_t *nodes[2])
{
	root->left = nodes[0];
	root->right = nodes[1];
	root->color = RED;
	root->left->color = BLACK;
	root->right->color = BLACK;

	root->left->parent = root;
	root->right->parent = root;

	if (root->key > nodes[0]->key) {
		root->right->key = root->key;
		root->key = root->left->key;
	} else {
		root->left->key = root->key;
	}
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
		free(new_nodes[1]);
		return 1;
	}

	if (place->key == new_nodes[0]->key)
		return 0;

	replace_external_node(place, new_nodes);
	rbt_insert_fixup(rbt, place);

	return 1;
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

	printf("%d[%s][%p (%p,%p)]\n", root->key, IS_RED(root) ? "RED" : "BLA", root, 
	                               &root->left, &root->right);

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
/******************************************************************************/

int _rbt_insert_helper(rbt_t *rbt, int key, void *value, tdata_t *tdata)
{
	rbt_node_t *node_stack[50], *siblings_stack[50];
	int stack_top = 0;
	tm_begin_ret_t status;
	rbt_node_t *place;
	int retries = -1, ret = 0;
	rbt_node_t *connection_point;
	int connection_point_stack_index = -1;
	int i;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
//		pthread_spin_lock(&rbt->rbt_lock);
//		place = _traverse(rbt, key);
//		ret = _insert(rbt, place, new_nodes);
//		pthread_spin_unlock(&rbt->rbt_lock);
//		return ret;
		return 0;
	}

	// Asynchronized traversal. If key is already there we can safely return.
	ht_reset(tdata->ht);
	place = _traverse_with_stack(rbt, key, node_stack, &stack_top);
	if (place && place->key == key)
		return 0;
//	printf("======================================================\n");
//	printf("KEY = %d\n", key);
//	printf("------------------------------------------------------\n");
//	ht_print(tdata->ht);
//	printf("------------------------------------------------------\n");
//	rbt_print_rec(rbt->root, 0);
//	printf("======================================================\n");
//	return 0;


	// For now let's ignore empty tree case and case with only one node in the tree.
	assert(stack_top >= 2);

	i=0;
	int hops = 0;
	int stack_current = 0;
	int i_want_to_print = 0;

	// Create the new RED internal node with two external children
	rbt_node_t *new_internal = rbt_node_new(key, RED, value);
	if (key < place->key) {
		new_internal->left = rbt_node_new(key, BLACK, value);
		new_internal->right = place;
	} else {
		new_internal->key = place->key;
		new_internal->right = rbt_node_new(key, BLACK, value);
		new_internal->left = place;
	}

	ht_insert(tdata->ht, &place->left, NULL);
	ht_insert(tdata->ht, &place->right, NULL);

	rbt_node_t *tree_copy_root = new_internal;
	rbt_node_t *grandfather = node_stack[stack_top-2];
	rbt_node_t *parent = node_stack[stack_top-1];
	stack_current = stack_top - 2;
	connection_point = parent;
	connection_point_stack_index = stack_top - 1;

	while (IS_RED(tree_copy_root) && IS_RED(parent)) {
		// Parent is the root.
		if (node_stack[0] == parent) {
			rbt_node_t *parent_cp = rbt_node_new_copy(parent, tdata);
			parent_cp->color = BLACK;
			if (key <= parent_cp->key)
				parent_cp->left = tree_copy_root;
			else
				parent_cp->right = tree_copy_root;
			tree_copy_root = parent_cp;
			connection_point = NULL;
			break;
		}

		// Copy the old parent and grandfather
		rbt_node_t *grandfather_cp = rbt_node_new_copy(grandfather, tdata);
		ht_insert(tdata->ht, &grandfather->left, grandfather_cp->left);
		ht_insert(tdata->ht, &grandfather->right, grandfather_cp->right);

		rbt_node_t *parent_cp = rbt_node_new_copy(parent, tdata);
		ht_insert(tdata->ht, &parent->left, parent_cp->left);
		ht_insert(tdata->ht, &parent->right, parent_cp->right);

		// Connect the copied nodes with the tree copy
		if (key <= grandfather_cp->key) grandfather_cp->left = parent_cp;
		else                            grandfather_cp->right = parent_cp;
		if (key <= parent_cp->key) parent_cp->left = tree_copy_root;
		else                       parent_cp->right = tree_copy_root;

		// Now perform the required action on the tree copy
		if (IS_RED(grandfather_cp->left) && IS_RED(grandfather_cp->right)) {
			hops++;

			// Copy the sibling (we have already copied parent)
			rbt_node_t *sibling_cp, *sibling;
			if (key <= grandfather_cp->key) {
				sibling = grandfather_cp->right;
				sibling_cp = rbt_node_new_copy(sibling, tdata);
				ht_insert(tdata->ht, &sibling->left, sibling_cp->left);
				ht_insert(tdata->ht, &sibling->right, sibling_cp->right);
				grandfather_cp->right = sibling_cp;
			} else {
				sibling = grandfather_cp->left;
				sibling_cp = rbt_node_new_copy(sibling, tdata);
				ht_insert(tdata->ht, &sibling->left, sibling_cp->left);
				ht_insert(tdata->ht, &sibling->right, sibling_cp->right);
				grandfather_cp->left = sibling_cp;
			}

			grandfather_cp->color = RED;
			grandfather_cp->right->color = BLACK;
			grandfather_cp->left->color = BLACK;

			if (node_stack[0] == grandfather) {
				grandfather_cp->color = BLACK;

				tree_copy_root = grandfather_cp;
				connection_point = NULL;
				break;
			}

			parent = node_stack[stack_current-1];
			grandfather = (stack_current-2 >= 0) ? node_stack[stack_current-2] : NULL;
			stack_current -= 2;

			tree_copy_root = grandfather_cp;
			connection_point = parent;
			connection_point_stack_index = stack_current - 1;
			continue;
		} else {
			if (key <= grandfather_cp->key) { // Father is left child
				if (key <= parent_cp->key) {
					// Right rotation
					grandfather_cp->left = parent_cp->right;
					parent_cp->right = grandfather_cp;

					parent_cp->color = BLACK;
					grandfather_cp->color = RED;

					tree_copy_root = parent_cp;
				} else {
					// 1. Left rotation
					parent_cp->right = tree_copy_root->left;
					tree_copy_root->left = parent_cp;
					grandfather_cp->left = tree_copy_root;

					// 2. Right rotation
					grandfather_cp->left = tree_copy_root->right;
					tree_copy_root->right = grandfather_cp;

					tree_copy_root->color = BLACK;
					grandfather_cp->color = RED;
				}

				if (node_stack[0] == grandfather) {
					connection_point = NULL;
				} else {
					connection_point = node_stack[stack_current-1];
					connection_point_stack_index = stack_current - 1;
				}
			} else {                          // Father is right child
				if (key > parent_cp->key) {
					// Left rotation
					grandfather_cp->right= parent_cp->left;
					parent_cp->left = grandfather_cp;

					parent_cp->color = BLACK;
					grandfather_cp->color = RED;

					tree_copy_root = parent_cp;
				} else {
					// 1. Right rotation
					parent_cp->left = tree_copy_root->right;
					tree_copy_root->right = parent_cp;
					grandfather_cp->right = tree_copy_root;

					// 2. Left rotation
					grandfather_cp->right = tree_copy_root->left;
					tree_copy_root->left = grandfather_cp;

					tree_copy_root->color = BLACK;
					grandfather_cp->color = RED;
				}

				if (node_stack[0] == grandfather) {
					connection_point = NULL;
				} else {
					connection_point = node_stack[stack_current-1];
					connection_point_stack_index = stack_current - 1;
				}
			}
			// After rotations we are done!
			break;
		}
	}

//	if (hops == 1 && i_want_to_print) {
//		printf("======================================================\n");
//		printf("KEY = %d\n", key);
//		rbt_print_rec(connection_point, 0);
//		printf("------------------------------------------------------\n");
//		rbt_print_rec(tree_copy_root, 0);
//		printf("------------------------------------------------------\n");
//		ht_print(tdata->ht);
//		printf("======================================================\n");
//	}

validate_and_connect_copy:
	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Verify that the access path is untouched.
		if (node_stack[stack_top]->left != NULL || node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (rbt->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			rbt_node_t *curr = rbt->root;
			while (curr && curr != connection_point)
				curr = (key <= curr->key) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		}
	
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				rbt_node_t **np = tdata->ht->entries[i][j];
				rbt_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			rbt->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		if (ABORT_IS_EXPLICIT(status) && 
		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
			tdata->tx_aborts_explicit_validation++;
			goto try_from_scratch;
		} else {
			goto validate_and_connect_copy;
		}
	}

	return 1;
}

int _rbt_insert_helper_warmup(rbt_t *rbt, rbt_node_t *new_nodes[2], tdata_t *tdata)
{
	rbt_node_t *place = _traverse(rbt, new_nodes[0]->key);
	return _insert(rbt, place, new_nodes);
}

/**
 * `x` points to the node that replaced the deleted one. 
 **/
static void rbt_delete_fixup(rbt_t *rbt, rbt_node_t *x)
{
	while (x != rbt->root && IS_BLACK(x)) {
		if (x == x->parent->left) { /* x is left child */
			rbt_node_t *w = x->parent->right;
			if (IS_RED(w)) { /* Case 1 */
				/* x's sibling is RED */
				w->color = BLACK;
				x->parent->color = RED;
				rbt_rotate_left(rbt, x->parent);
				w = x->parent->right;
			}

			if (IS_BLACK(w->left) && IS_BLACK(w->right)) {
				/* Case 2 */
				/* x's sibling is RED with two BLACK children */
				w->color = RED;
				x = x->parent;
			} else {
				/* x's sibling is RED with two RED or RED and BLACK children */
				if (IS_BLACK(w->right)) { /* Case 4 */
					w->left->color = BLACK;
					w->color = RED;
					rbt_rotate_right(rbt, w);
					w = x->parent->right;
				}
				/* Case 3 */
				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->right->color = BLACK;
				rbt_rotate_left(rbt, x->parent);
				x = rbt->root;
			}
		} else {                       /* x is right child */
			rbt_node_t *w = x->parent->left;
			if (IS_RED(w)) { /* Case 1 */
				w->color = BLACK;
				x->parent->color = RED;
				rbt_rotate_right(rbt, x->parent);
				w = x->parent->left;
			}

			if (IS_BLACK(w->right) && IS_BLACK(w->left)) {
				/* Case 2 */
				/* x's sibling is RED with two BLACK children */
				w->color = RED;
				x = x->parent;
			} else {
				/* x's sibling is RED with two RED or RED and BLACK children */
				if (IS_BLACK(w->left)) { /* Case 4 */
					w->right->color = BLACK;
					w->color = RED;
					rbt_rotate_left(rbt, w);
					w = x->parent->left;
				}
				/* Case 3 */
				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->left->color = BLACK;
				rbt_rotate_right(rbt, x->parent);
				x = rbt->root;
			}
		}
	}

	if (IS_RED(x))
		x->color = BLACK;
}

static int _delete(rbt_t *rbt, int key, rbt_node_t *z, rbt_node_t *nodes_to_free[2])
{
	if (!z || z->key != key)
		return 0;

	if (z == rbt->root) {
		rbt->root = NULL;
		nodes_to_free[0] = z;
		nodes_to_free[1] = NULL;
		return 1;
	}

	color_t delete_node_color = z->parent->color;

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
	}

	if (delete_node_color == BLACK)
		rbt_delete_fixup(rbt, sibling);

	return 1;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t *nodes_to_free[2],
                              tdata_t *tdata)
{
	rbt_node_t *node_stack[50], *siblings_stack[50];
	int stack_top = 0;
	tm_begin_ret_t status;
	rbt_node_t *place;
	int retries = -1, ret = 0;
	rbt_node_t *connection_point;
	int connection_point_stack_index = -1;
	int i;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
//		pthread_spin_lock(&rbt->rbt_lock);
//		place = _traverse(rbt, key);
//		ret = _delete(rbt, key, place, nodes_to_free);
//		pthread_spin_unlock(&rbt->rbt_lock);
//		return ret;
		return 0;
	}

	/* Asynchronized traversal. If key is not there we can safely return. */
	ht_reset(tdata->ht);
	place = _traverse_with_stack(rbt, key, node_stack, &stack_top);
	if (place && place->key != key)
		return 0;

	// FIXME
	if (stack_top <= 3)
		return 0;

	// No need to copy here.
	rbt_node_t *parent = node_stack[stack_top-1];
	rbt_node_t *place_sibling;
	if (key <= parent->key) {
		place_sibling = parent->right;
		ht_insert(tdata->ht, &parent->right, place_sibling);
	} else {
		place_sibling = parent->left;
		ht_insert(tdata->ht, &parent->left, place_sibling);
	}

//	if (!place_sibling)
//		return 0;
	rbt_node_t *tree_copy_root = rbt_node_new_copy(place_sibling, tdata);
	ht_insert(tdata->ht, &place_sibling->left, tree_copy_root->left);
	ht_insert(tdata->ht, &place_sibling->right, tree_copy_root->right);

	connection_point = node_stack[stack_top-2];
	connection_point_stack_index = stack_top - 2;

	int stack_current = stack_top-2; // Shows at the grandparent of the deleted leaf.
	int hops = 0;
	int i_want_to_print = 0;

	color_t deleted_node_color = parent->color;
	if (deleted_node_color == RED) {
		goto validate_and_connect_copy;
	} else if (IS_RED(tree_copy_root)) {
		tree_copy_root->color = BLACK;
		goto validate_and_connect_copy;
	}

	while (IS_BLACK(tree_copy_root)) {
		parent = node_stack[stack_current];

		// FIXME
		if (parent == node_stack[0])
			return 0;

		// Copy parent
		rbt_node_t *parent_cp = rbt_node_new_copy(parent, tdata);

		if (key <= parent_cp->key) {
			rbt_node_t *w = parent_cp->right;
			ht_insert(tdata->ht, &parent->right, w);

			rbt_node_t *w_cp = rbt_node_new_copy(w, tdata);
			ht_insert(tdata->ht, &w->left, w_cp->left);
			ht_insert(tdata->ht, &w->right, w_cp->right);

			if (IS_RED(w_cp)) {
				rbt_node_t *w_left_cp = rbt_node_new_copy(w_cp->left, tdata);
				ht_insert(tdata->ht, &w_cp->left->left, w_left_cp->left);
				ht_insert(tdata->ht, &w_cp->left->right, w_left_cp->right);

				parent_cp->right = w_cp;
				w_cp->left = w_left_cp;

				// Left rotation
				parent_cp->right = w_left_cp;
				w_cp->left = parent_cp;
				// Recoloring
				w_cp->color = BLACK;
				parent_cp->color = RED;

				if (IS_BLACK(w_left_cp->left) && IS_BLACK(w_left_cp->right)) {
					// Recoloring
					w_left_cp->color = RED;
					parent_cp->color = BLACK;
				} else if (IS_RED(w_left_cp->right)) {
					rbt_node_t *w_left_right_cp = rbt_node_new_copy(w_left_cp->right, tdata);
					ht_insert(tdata->ht, &w_left_cp->right->left, w_left_right_cp->left);
					ht_insert(tdata->ht, &w_left_cp->right->right, w_left_right_cp->right);

					w_left_cp->right = w_left_right_cp;

					// Left rotation
					parent_cp->right = w_left_cp->left;
					w_left_cp->left = parent_cp;
					w_cp->left = w_left_cp;
					// Recoloring
					w_left_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					w_left_right_cp->color = BLACK;
				} else { // IS_RED(w_left_cp->left)
					rbt_node_t *w_left_left_cp = rbt_node_new_copy(w_left_cp->left, tdata);
					ht_insert(tdata->ht, &w_left_cp->left->left, w_left_left_cp->left);
					ht_insert(tdata->ht, &w_left_cp->left->right, w_left_left_cp->right);

					w_left_cp->left = w_left_left_cp;

					// Right rotation
					w_left_cp->left = w_left_left_cp->right;
					w_left_left_cp->right = w_left_cp;
					parent_cp->right = w_left_left_cp;
					// Recoloring
					w_left_left_cp->color = BLACK;
					w_left_cp->color = RED;
					// Left rotation
					parent_cp->right = w_left_left_cp->left;
					w_left_left_cp->left = parent_cp;
					w_cp->left = w_left_left_cp;
					// Recoloring
					w_left_left_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					w_left_cp->color = BLACK;
				}

				parent_cp->left = tree_copy_root;
				tree_copy_root = w_cp;
				connection_point = node_stack[stack_current-1];
				connection_point_stack_index = stack_current - 1;
				break;
			}
	
			if (IS_BLACK(w_cp)) {
				if (IS_BLACK(w_cp->left) && IS_BLACK(w_cp->right)) {
					w_cp->color = RED;
					parent_cp->right = w_cp;
					parent_cp->left = tree_copy_root;
					tree_copy_root = parent_cp;

					stack_current--;
					connection_point = node_stack[stack_current];
					connection_point_stack_index = stack_current;

					hops++;

					if (IS_RED(tree_copy_root)) {
						tree_copy_root->color = BLACK;
						break;
					}
				} else {
					if (IS_RED(w_cp->right)) {
						rbt_node_t *w_right_cp = rbt_node_new_copy(w_cp->right, tdata);
						ht_insert(tdata->ht, &w_cp->right->left, w_right_cp->left);
						ht_insert(tdata->ht, &w_cp->right->right, w_right_cp->right);

						parent_cp->left = tree_copy_root;
						parent_cp->right = w_cp;
						w_cp->right = w_right_cp;

						// Left rotation
						parent_cp->right = w_cp->left;
						w_cp->left = parent_cp;
						// Recoloring
						w_cp->color = parent_cp->color;
						parent_cp->color = BLACK;
						w_right_cp->color = BLACK;

						tree_copy_root = w_cp;
						connection_point = node_stack[stack_current-1];
						connection_point_stack_index = stack_current - 1;

						break;
					} else {
						rbt_node_t *w_left_cp = rbt_node_new_copy(w_cp->left, tdata);
						ht_insert(tdata->ht, &w_cp->left->left, w_left_cp->left);
						ht_insert(tdata->ht, &w_cp->left->right, w_left_cp->right);

						parent_cp->left = tree_copy_root;
						parent_cp->right = w_cp;
						w_cp->left = w_left_cp;

						// Right rotation
						w_cp->left = w_left_cp->right;
						w_left_cp->right = w_cp;
						parent_cp->right = w_left_cp;
						// Left rotation
						parent_cp->right = w_left_cp->left;
						w_left_cp->left = parent_cp;
						// Recoloring
						w_left_cp->color = parent_cp->color;
						parent_cp->color = BLACK;
						w_cp->color = BLACK;

						tree_copy_root = w_left_cp;
						connection_point = node_stack[stack_current-1];
						connection_point_stack_index = stack_current - 1;

						break;
					}
				}
			}
		} else { // key > parent_cp->key
			rbt_node_t *w = parent_cp->left;
			ht_insert(tdata->ht, &parent->left, w);

			rbt_node_t *w_cp = rbt_node_new_copy(w, tdata);
			ht_insert(tdata->ht, &w->left, w_cp->left);
			ht_insert(tdata->ht, &w->right, w_cp->right);

			if (IS_RED(w_cp)) {
				rbt_node_t *w_right_cp = rbt_node_new_copy(w_cp->right, tdata);
				ht_insert(tdata->ht, &w_cp->right->left, w_right_cp->left);
				ht_insert(tdata->ht, &w_cp->right->right, w_right_cp->right);

				parent_cp->left = w_cp;
				w_cp->right = w_right_cp;

				// Right rotation
				parent_cp->left = w_right_cp;
				w_cp->right = parent_cp;
				// Recoloring
				w_cp->color = BLACK;
				parent_cp->color = RED;

				if (IS_BLACK(w_right_cp->left) && IS_BLACK(w_right_cp->right)) {
					// Recoloring
					w_right_cp->color = RED;
					parent_cp->color = BLACK;
				} else if (IS_RED(w_right_cp->left)) {
					rbt_node_t *w_right_left_cp = rbt_node_new_copy(w_right_cp->left, tdata);
					ht_insert(tdata->ht, &w_right_cp->left->left,  w_right_left_cp->left);
					ht_insert(tdata->ht, &w_right_cp->left->right, w_right_left_cp->right);

					w_right_cp->left = w_right_left_cp;

					// Right rotation
					parent_cp->left = w_right_cp->right;
					w_right_cp->right = parent_cp;
					w_cp->right = w_right_cp;
					// Recoloring
					w_right_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					w_right_left_cp->color = BLACK;
				} else { // IS_RED(w_right_cp->right)
					rbt_node_t *w_right_right_cp = rbt_node_new_copy(w_right_cp->right, tdata);
					ht_insert(tdata->ht, &w_right_cp->right->left, w_right_right_cp->left);
					ht_insert(tdata->ht, &w_right_cp->right->right, w_right_right_cp->right);

					w_right_cp->right = w_right_right_cp;

					// Left rotation
					w_right_cp->right = w_right_right_cp->left;
					w_right_right_cp->left = w_right_cp;
					parent_cp->left = w_right_right_cp;
					// Recoloring
					w_right_right_cp->color = BLACK;
					w_right_cp->color = RED;
					// Right rotation
					parent_cp->left = w_right_right_cp->right;
					w_right_right_cp->right = parent_cp;
					w_cp->right = w_right_right_cp;
					// Recoloring
					w_right_right_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					w_right_cp->color = BLACK;
				}

				parent_cp->right = tree_copy_root;
				tree_copy_root = w_cp;
				connection_point = node_stack[stack_current-1];
				connection_point_stack_index = stack_current - 1;
				break;
			}

			if (IS_BLACK(w_cp)) {
				if (IS_BLACK(w_cp->left) && IS_BLACK(w_cp->right)) {
					w_cp->color = RED;
					parent_cp->left= w_cp;
					parent_cp->right = tree_copy_root;
					tree_copy_root = parent_cp;

					stack_current--;
					connection_point = node_stack[stack_current];
					connection_point_stack_index = stack_current;

					hops++;

					if (IS_RED(tree_copy_root)) {
						tree_copy_root->color = BLACK;
						break;
					}
				} else {
					if (IS_RED(w_cp->left)) {
						rbt_node_t *w_left_cp = rbt_node_new_copy(w_cp->left, tdata);
						ht_insert(tdata->ht, &w_cp->left->left, w_left_cp->left);
						ht_insert(tdata->ht, &w_cp->left->right, w_left_cp->right);

						parent_cp->right = tree_copy_root;
						parent_cp->left = w_cp;
						w_cp->left = w_left_cp;

						// Left rotation
						parent_cp->left = w_cp->right;
						w_cp->right = parent_cp;
						// Recoloring
						w_cp->color = parent_cp->color;
						parent_cp->color = BLACK;
						w_left_cp->color = BLACK;

						tree_copy_root = w_cp;
						connection_point = node_stack[stack_current-1];
						connection_point_stack_index = stack_current - 1;

						break;
					} else {
						rbt_node_t *w_right_cp = rbt_node_new_copy(w_cp->right, tdata);
						ht_insert(tdata->ht, &w_cp->right->left, w_right_cp->left);
						ht_insert(tdata->ht, &w_cp->right->right, w_right_cp->right);

						parent_cp->right = tree_copy_root;
						parent_cp->left = w_cp;
						w_cp->right = w_right_cp;

						// Left rotation
						w_cp->right = w_right_cp->left;
						w_right_cp->left = w_cp;
						parent_cp->left = w_right_cp;
						// Right rotation
						parent_cp->left = w_right_cp->right;
						w_right_cp->right = parent_cp;
						// Recoloring
						w_right_cp->color = parent_cp->color;
						parent_cp->color = BLACK;
						w_cp->color = BLACK;

						tree_copy_root = w_right_cp;
						connection_point = node_stack[stack_current-1];
						connection_point_stack_index = stack_current - 1;

						break;

					}

				}
			}
		}
	}

//	if (hops == 0 && i_want_to_print) {
//		printf("========================================================\n");
//		printf("Key = %d Hops = %d\n", key, hops);
//		rbt_print_rec(connection_point, 0);
//		printf("--------------------------------------------------------\n");
//		rbt_print_rec(tree_copy_root, 0);
//		printf("========================================================\n");
//	}


validate_and_connect_copy:
	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Verify that the access path is untouched.
		if (node_stack[stack_top]->left != NULL || node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (rbt->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			rbt_node_t *curr = rbt->root;
			while (curr && curr != connection_point)
				curr = (key <= curr->key) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		}
	
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				rbt_node_t **np = tdata->ht->entries[i][j];
				rbt_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			rbt->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		if (ABORT_IS_EXPLICIT(status) && 
		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
			tdata->tx_aborts_explicit_validation++;
			goto try_from_scratch;
		} else {
			goto validate_and_connect_copy;
		}
	}

	return 1;
}

static int key_in_min_path, key_in_max_path;
static int bh;
static int paths_with_bh_diff;
static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes, red_nodes, black_nodes;
static int red_red_violations, bst_violations;
static void _rbt_validate(rbt_node_t *root, int _bh, int _th)
{
	if (!root)
		return;

	rbt_node_t *left = root->left;
	rbt_node_t *right = root->right;

	total_nodes++;
	black_nodes += (root->color == BLACK);
	red_nodes += (root->color == RED);
	_th++;
	_bh += (root->color == BLACK);

	/* BST violation? */
	if (left && left->key > root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* Red-Red violation? */
	if (root->color == RED && (left->color == RED || right->color == RED))
		red_red_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (!left || !right) {
		total_paths++;
		if (bh == -1)
			bh = _bh;
		else if (_bh != bh)
			paths_with_bh_diff++;

		if (_th <= min_path_len) {
			min_path_len = _th;
			key_in_min_path = root->key;
		}
		if (_th >= max_path_len) {
			max_path_len = _th;
			key_in_max_path = root->key;
		}
	}

	/* Check subtrees. */
	if (left)
		_rbt_validate(left, _bh, _th);
	if (right)
		_rbt_validate(right, _bh, _th);
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

	_rbt_validate(root, 0, 0);

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
	printf("  Key in min path: %d\n", key_in_min_path);
	printf("  Key in max path: %d\n", key_in_max_path);
	printf("\n");

	return check_rbt;
}

static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	rbt_node_t *new_nodes[2];
	tdata_t *tdata = tdata_new(-1);
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		new_nodes[0] = rbt_node_new(key, BLACK, NULL);
		new_nodes[1] = rbt_node_new(key, BLACK, NULL);

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
	// Pre allocate a large amount of nodes for each thread
	per_thread_node_allocators[tid] = malloc(10000000*sizeof(rbt_node_t));
	memset(per_thread_node_allocators[tid], 0, 10000000*sizeof(rbt_node_t));

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
	tdata_t *tdata = thread_data;

	ret = _rbt_insert_helper(rbt, key, value, tdata);

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
	return "links_bu_rcu_htm_external";
}
