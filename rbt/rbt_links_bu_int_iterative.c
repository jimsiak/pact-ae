#include <assert.h>

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm.h"
#	if !defined(TX_NUM_RETRIES)
#		define TX_NUM_RETRIES 20
#	endif
#endif

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"
#include "rbt_links_td_ext_thread_data.h" /* verbose stats */

typedef enum {
	RED = 0,
	BLACK
} color_t;

typedef struct rbt_node {
	color_t color;
	int key;
	struct rbt_node *parent,
	                *left,
	                *right;

	void *data;

//	char padding[CACHE_LINE_SIZE - sizeof(color_t) - sizeof(int) -
//	             sizeof(void *) - 3 * sizeof(struct rbt_node *)];
//} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;
} rbt_node_t;

typedef struct {
	rbt_node_t *root,
	           *sentinel;

#	if defined(SYNC_CG_HTM) || defined(SYNC_CG_SPINLOCK)
	pthread_spinlock_t rbt_lock;
#	endif
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
	node->data = data;

	return node;
}

rbt_t *_rbt_new_helper()
{
	rbt_t *rbt;

	XMALLOC(rbt, 1);
	rbt->sentinel = rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	rbt->sentinel->parent = rbt->sentinel;
//	rbt->sentinel->left = rbt->sentinel;
//	rbt->sentinel->right = rbt->sentinel;
	rbt->sentinel->left = rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	rbt->sentinel->right= rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	rbt->root = rbt->sentinel;

#	if defined(SYNC_CG_HTM) || defined(SYNC_CG_SPINLOCK)
	if (pthread_spin_init(&rbt->rbt_lock, PTHREAD_PROCESS_SHARED)) {
		perror("pthread_spin_init");
		exit(1);
	}
#endif

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

/**
 * Returns 1 if found, else 0.
 **/
int _rbt_lookup_helper(rbt_t *rbt, int key)
{
	rbt_node_t *p = rbt->root;
	while (!IS_SENTINEL_NODE(p)) {
		if (key < p->key) {
			p = p->left;
		} else if (key > p->key) {
			p = p->right;
		} else {
			return 1;
		}
	}

	return 0;
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
 *  Returns the inserted node on success, NULL if node is already there.
 **/
static rbt_node_t *_do_binary_insert(rbt_t *rbt, rbt_node_t *new_node)
{
	rbt_node_t *node, *x, *y;
	int key = new_node->key;

	x = rbt->root;
	y = rbt->sentinel;

	/* Move down to the position of the new node. */
	while (!IS_SENTINEL_NODE(x)) {
		y = x;
		if (key < x->key)
			x = x->left;
		else if (key > x->key)
			x = x->right;
		else 
			return NULL;
	}

	/* Place the new node in its position. */
	node = new_node;
	node->parent = y;
	if (IS_SENTINEL_NODE(y)) {
		rbt->root = node;
	} else {
		if (node->key < y->key)
			y->left = node;
		else
			y->right = node;
	}

	return node;
}

int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *new_node)
{
	int real_insertions = 0;

	rbt_node_t *node = _do_binary_insert(rbt, new_node);
	if (node) {
		real_insertions++;
		rbt_insert_fixup(rbt, node);
	}

	return real_insertions;
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

static void _do_rbt_delete(rbt_t *rbt, rbt_node_t *z, rbt_node_t **node_to_free)
{
	rbt_node_t *x, *y;

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

	*node_to_free = y;
}

static rbt_node_t *find_node(rbt_t *rbt, int key)
{
	rbt_node_t *p = rbt->root;

	while (!IS_SENTINEL_NODE(p)) {
		if (key < p->key) {
			p = p->left;
		} else if (key > p->key) {
			p = p->right;
		} else {
			return p;
		}
	}

	return NULL;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t **node_to_free)
{
	int ret = 0;
	rbt_node_t *node = find_node(rbt, key);
	if (node) {
		_do_rbt_delete(rbt, node, node_to_free);
		ret = 1;
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

		ret = _rbt_insert_helper(rbt, node);
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
	td_ext_thread_data_t *data = td_ext_thread_data_new(tid);

#	if defined(SYNC_CG_HTM)
	data->priv = tx_thread_data_new(tid);
#	endif

	return data;
}

void rbt_thread_data_print(void *thread_data)
{
	td_ext_thread_data_t *tdata = thread_data;

	td_ext_thread_data_print(tdata);

#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(tdata->priv);
#	endif

	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
	td_ext_thread_data_t *_d1 = d1, *_d2 = d2, *_dst = dst;

	td_ext_thread_data_add(_d1, _d2, _dst);
#	if defined(SYNC_CG_HTM)
	tx_thread_data_add(_d1->priv, _d2->priv, _dst->priv);
#	endif
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	td_ext_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	int naborts = tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_lookup_helper(rbt, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	int tx_ret = tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret;
	rbt_node_t *node;
	td_ext_thread_data_t *tdata = thread_data;

	node = rbt_node_new(key, RED, value);
	node->left = rbt_node_new(SENTINEL_KEY, BLACK, NULL);
	node->right = rbt_node_new(SENTINEL_KEY, BLACK, NULL);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_insert_helper(rbt, node);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

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
	td_ext_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_delete_helper(rbt, key, &node_to_free);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	if (ret) {
//		if (IS_SENTINEL_NODE(node_to_free->left))
//			free(node_to_free->left);
//		if (IS_SENTINEL_NODE(node_to_free->right))
//			free(node_to_free->right);
		free(node_to_free);
	}

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
	return "links_bu_internal";
}
