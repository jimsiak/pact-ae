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
	color_t color;
	int key;
	void *data;
	struct rbt_node *parent,
	                *left, *right,
	                *prev, *succ;

	int live;

	char padding[CACHE_LINE_SIZE - sizeof(color_t) - 2 * sizeof(int) -
	             sizeof(void *) - 5 * sizeof(struct rbt_node *)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;

typedef struct {
	rbt_node_t *root,
	           *sentinel;

	pthread_spinlock_t rbt_lock;
} rbt_t;

#define IS_BLACK(node) ( !(node) || (node)->color == BLACK )
#define IS_RED(node) ( !IS_BLACK(node) )

#define SENTINEL_KEY -999999
#define IS_SENTINEL_NODE(n) (n->key == SENTINEL_KEY)

static rbt_node_t *rbt_node_new(int key, color_t color, void *data)
{
	rbt_node_t *node;
	
	XMALLOC(node, 1);
	node->color = color;
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
	rbt->sentinel = rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	rbt->sentinel->parent = rbt->sentinel;
	rbt->sentinel->left = rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	rbt->sentinel->right= rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	rbt->root = rbt->sentinel;

	pthread_spin_init(&rbt->rbt_lock, PTHREAD_PROCESS_SHARED);

	return rbt;
}

static void rbt_rotate_left(rbt_t *rbt, rbt_node_t *x)
{
	rbt_node_t *y = x->right;
	
	x->right = y->left;
	if (!IS_SENTINEL_NODE(y->left))
		y->left->parent = x;

	y->parent = x->parent;
	if (IS_SENTINEL_NODE(x->parent))
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
	if (!IS_SENTINEL_NODE(x->right))
		x->right->parent = y;

	x->parent = y->parent;
	if (IS_SENTINEL_NODE(y->parent))
		rbt->root = x;
	else if (y == y->parent->right)
		y->parent->right = x;
	else
		y->parent->left = x;
	
	x->right = y;
	y->parent = x;
}

static inline void _lookup_verify(rbt_node_t *node, int key)
{
	if (!node || !node->live)
		TX_ABORT(ABORT_VALIDATION_FAILURE);

	if (node->key == key)
		return;

	if (key < node->key) {
		if (!IS_SENTINEL_NODE(node->left))
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		rbt_node_t *prev = node->prev;
		if (prev && key <= prev->key)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	} else {
		if (!IS_SENTINEL_NODE(node->right))
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		rbt_node_t *succ = node->succ;
		if (succ && key >= succ->key)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	}
}

/** Returns a pointer to the node that contains `key` or
 *  to the parent of the node that would contain it.
 **/
static rbt_node_t *_traverse(rbt_t *rbt, int key)
{
	rbt_node_t *prev = rbt->sentinel, *curr = rbt->root;

	while (!IS_SENTINEL_NODE(curr)) {
		prev = curr;
		if (key < curr->key)
			curr = curr->left;
		else if (key > curr->key)
			curr = curr->right;
		else
			return curr;
	}

	return prev;
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
		ret = (place->key == key);
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
		ret = (place->key == key);

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	return ret;
}

/**
 * z is the newly inserted node.
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
 *  Returns 0 if key was already there, 1 otherwise.
 **/
static inline int _insert(rbt_t *rbt, rbt_node_t *place, rbt_node_t *new_node)
{
	rbt_node_t *prev = NULL, *succ = NULL;

	if (place->key == new_node->key)
		return 0;

	/* Place the new node in its position. */
	new_node->parent = place;
	new_node->live = 1;
	if (IS_SENTINEL_NODE(place)) {
		rbt->root = new_node;
		place->prev = NULL;
		place->succ = NULL;
	} else {
		if (new_node->key < place->key) {
			place->left = new_node;
			succ = place;
			if (succ) prev = succ->prev;

		} else {
			place->right = new_node;
			prev = place;
			if (prev) succ = prev->succ;
		}

		new_node->prev = prev;
		new_node->succ = succ;
		if (prev) prev->succ = new_node;
		if (succ) succ->prev = new_node;
	}

	rbt_insert_fixup(rbt, new_node);
	return 1;
}

int _rbt_insert_helper_warmup(rbt_t *rbt, rbt_node_t *new_node)
{
	rbt_node_t *place = _traverse(rbt, new_node->key);
	return _insert(rbt, place, new_node);
}

int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *new_node, tdata_t *tdata)
{
	tm_begin_ret_t status;
	rbt_node_t *place;
	int retries = -1, ret = 0;

try_from_scratch:

	/* Global lock fallback. */
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&rbt->rbt_lock);
		place = _traverse(rbt, new_node->key);
		ret = _insert(rbt, place, new_node);
		pthread_spin_unlock(&rbt->rbt_lock);
		return ret;
	}

	/* Asynchronized traversal. */
	place = _traverse(rbt, new_node->key);

	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		/* _lookup_verify() will abort if verification fails. */
		_lookup_verify(place, new_node->key);
		ret = _insert(rbt, place, new_node);

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		goto try_from_scratch;
	}

	return ret;

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

static rbt_node_t *rbt_minimum_node(rbt_node_t *root)
{
	rbt_node_t *ret;

	if (IS_SENTINEL_NODE(root))
		return root;

	for (ret = root; !IS_SENTINEL_NODE(ret->left); ret = ret->left)
		;

	return ret;
}

static int _delete(rbt_t *rbt, int key, rbt_node_t *z, rbt_node_t **node_to_free)
{
	rbt_node_t *x, *y;

	if (z->key != key)
		return 0;

	/**
	 * 2 cases for z:
	 *   - zero or one child: z is to be removed.
	 *   - two children: the leftmost node of z's 
	 *     right subtree is to be removed.
	 **/
	if (IS_SENTINEL_NODE(z->left) || IS_SENTINEL_NODE(z->right))
		y = z;
	else
		y = rbt_minimum_node(z->right);

	x = y->left;
	if (IS_SENTINEL_NODE(x))
		x = y->right;

	/* replace y with x */
	x->parent = y->parent;
	if (IS_SENTINEL_NODE(y->parent)) {
		rbt->root = x;
	} else {
		if (y == y->parent->left)
			y->parent->left = x;
		else
			y->parent->right = x;
	}

	if (y != z) {
		z->key = y->key;
		z->data = y->data;
	}

	if (IS_BLACK(y))
		rbt_delete_fixup(rbt, x);

	rbt_node_t *prev = y->prev;
	rbt_node_t *succ = y->succ;
	if (prev) prev->succ = succ;
	if (succ) succ->prev = prev;
	y->live = 0;
	*node_to_free = y;

	return 1;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t **node_to_free,
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
		ret = _delete(rbt, key, place, node_to_free);
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
		ret = _delete(rbt, key, place, node_to_free);

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
static int total_nodes, red_nodes, black_nodes;
static int red_red_violations, bst_violations;
static void _rbt_validate(rbt_node_t *root, int _bh, int _th)
{
	if (IS_SENTINEL_NODE(root))
		return;

	rbt_node_t *left = root->left;
	rbt_node_t *right = root->right;

	total_nodes++;
	black_nodes += (root->color == BLACK);
	red_nodes += (root->color == RED);
	_th++;
	_bh += (root->color == BLACK);

	/* BST violation? */
	if (!IS_SENTINEL_NODE(left) && left->key >= root->key)
		bst_violations++;
	if (!IS_SENTINEL_NODE(right) && right->key <= root->key)
		bst_violations++;

	/* Red-Red violation? */
	if (root->color == RED && (left->color == RED || right->color == RED))
		red_red_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (IS_SENTINEL_NODE(left) || IS_SENTINEL_NODE(right)) {
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
	if (!IS_SENTINEL_NODE(left))
		_rbt_validate(left, _bh, _th);
	if (!IS_SENTINEL_NODE(right))
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
	printf("\n");

	return check_rbt;
}

static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	rbt_node_t *node;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		node = rbt_node_new(key, RED, NULL);
		node->left = rbt_node_new(SENTINEL_KEY, BLACK, NULL);
		node->right= rbt_node_new(SENTINEL_KEY, BLACK, NULL);

		ret = _rbt_insert_helper_warmup(rbt, node);
		nodes_inserted += ret;

		if (!ret) {
			free(node->left);
			free(node->right);
			free(node);
		}
	}

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

	printf("%d[%s]\n", root->key, IS_RED(root) ? "RED" : "BLA");

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
	rbt_node_t *node;
	tdata_t *tdata = thread_data;

	node = rbt_node_new(key, RED, value);
	node->left = rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	node->right = rbt_node_new(SENTINEL_KEY, BLACK, NULL);

	ret = _rbt_insert_helper(rbt, node, tdata);

	if (!ret) {
		free(node->left);
		free(node->right);
		free(node);
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
	return "links_bu_cop_internal";
}
