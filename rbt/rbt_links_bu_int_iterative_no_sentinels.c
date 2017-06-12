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
	void *data;
	struct rbt_node *left, *right;

//	char padding[CACHE_LINE_SIZE - sizeof(color_t) - sizeof(int) -
//	             sizeof(void *) - 3 * sizeof(struct rbt_node *)];
//} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;
} rbt_node_t;

typedef struct {
	rbt_node_t *root;

#	if defined(SYNC_CG_HTM) || defined(SYNC_CG_SPINLOCK)
	pthread_spinlock_t rbt_lock;
#	endif
} rbt_t;

unsigned int next_node_to_allocate;
rbt_node_t *per_thread_node_allocators[88];

#define MAX_HEIGHT 50
#define IS_BLACK(node) ( !(node) || (node)->color == BLACK )
#define IS_RED(node) ( !IS_BLACK(node) )

static rbt_node_t *rbt_node_new(int key, color_t color, void *data)
{
	rbt_node_t *node;
	
	XMALLOC(node, 1);
	node->color = color;
	node->key = key;
	node->data = data;
	node->left = NULL;
	node->right = NULL;

	return node;
}

rbt_t *_rbt_new_helper()
{
	rbt_t *rbt;

	XMALLOC(rbt, 1);
	rbt->root = NULL;

#	if defined(SYNC_CG_HTM) || defined(SYNC_CG_SPINLOCK)
	if (pthread_spin_init(&rbt->rbt_lock, PTHREAD_PROCESS_SHARED)) {
		perror("pthread_spin_init");
		exit(1);
	}
#	endif

	return rbt;
}

static inline rbt_node_t *rbt_rotate_left(rbt_node_t *node)
{
	assert(node != NULL && node->right != NULL);

	rbt_node_t *node_right = node->right;

	node->right = node->right->left;
	node_right->left = node;

	return node_right;
}

static inline rbt_node_t *rbt_rotate_right(rbt_node_t *node)
{
	assert(node != NULL && node->left != NULL);

	rbt_node_t *node_left = node->left;

	node->left = node->left->right;
	node_left->right = node;

	return node_left;
}

/**
 * Traverses the tree `rbt` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 * In the case of an empty tree both `parent` and `leaf` are NULL.
 **/
static inline void _traverse(rbt_t *rbt, int key, rbt_node_t **parent,
                                                  rbt_node_t **leaf)
{
	*parent = NULL;
	*leaf = rbt->root;

	while (*leaf) {
		int leaf_key = (*leaf)->key;
		if (leaf_key == key)
			return;

		*parent = *leaf;
		*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}
static inline void _traverse_with_stack(rbt_t *rbt, int key,
                                        rbt_node_t *node_stack[MAX_HEIGHT],
                                        int *stack_top)
{
	rbt_node_t *parent, *leaf;

	parent = NULL;
	leaf = rbt->root;
	*stack_top = -1;

	while (leaf) {
		node_stack[++(*stack_top)] = leaf;

		int leaf_key = leaf->key;
		if (leaf_key == key)
			return;

		parent = leaf;
		leaf = (key < leaf_key) ? leaf->left : leaf->right;
	}
}

/**
 * Returns 1 if found, else 0.
 **/
int _rbt_lookup_helper(rbt_t *rbt, int key)
{
	rbt_node_t *parent, *leaf;
	_traverse(rbt, key, &parent, &leaf);
	return (leaf != NULL);
}

static void _insert_rebalance(rbt_t *rbt, int key, rbt_node_t *node_stack[MAX_HEIGHT],
                       int stack_top)
{
	rbt_node_t *parent, *grandparent, *grandgrandparent, *uncle;

	while (1) {
		if (stack_top <= 0) {
			rbt->root->color = BLACK;
			break;
		}

		parent = node_stack[stack_top--];
		if (IS_BLACK(parent))
			break;

		grandparent = node_stack[stack_top--];
		if (key < grandparent->key) {
			uncle = grandparent->right;
			if (IS_RED(uncle)) {
				parent->color = BLACK;
				uncle->color = BLACK;
				grandparent->color = RED;
				continue;
			}

			if (key < parent->key) {
				if (stack_top == -1) {
					rbt->root = rbt_rotate_right(grandparent);
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key)
						grandgrandparent->left = rbt_rotate_right(grandparent);
					else
						grandgrandparent->right = rbt_rotate_right(grandparent);
				}
				parent->color = BLACK;
				grandparent->color = RED;
			} else {
				grandparent->left = rbt_rotate_left(parent);
				if (stack_top == -1) {
					rbt->root = rbt_rotate_right(grandparent);
					rbt->root->color = BLACK;
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key) {
						grandgrandparent->left = rbt_rotate_right(grandparent);
						grandgrandparent->left->color = BLACK;
					} else {
						grandgrandparent->right = rbt_rotate_right(grandparent);
						grandgrandparent->right->color = BLACK;
					}
				}
				grandparent->color = RED;
			}
			break;
		} else {
			uncle = grandparent->left;
			if (IS_RED(uncle)) {
				parent->color = BLACK;
				uncle->color = BLACK;
				grandparent->color = RED;
				continue;
			}

			if (key > parent->key) {
				if (stack_top == -1) {
					rbt->root = rbt_rotate_left(grandparent);
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key)
						grandgrandparent->left = rbt_rotate_left(grandparent);
					else
						grandgrandparent->right = rbt_rotate_left(grandparent);
				}
				parent->color = BLACK;
				grandparent->color = RED;
			} else {
				grandparent->right = rbt_rotate_right(parent);
				if (stack_top == -1) {
					rbt->root = rbt_rotate_left(grandparent);
					rbt->root->color = BLACK;
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key) {
						grandgrandparent->left = rbt_rotate_left(grandparent);
						grandgrandparent->left->color = BLACK;
					} else {
						grandgrandparent->right = rbt_rotate_left(grandparent);
						grandgrandparent->right->color = BLACK;
					}
				}
				grandparent->color = RED;
			}
			break;
		}
	}
}

static int _insert(rbt_t *rbt, int key, void *data,
                   rbt_node_t *node_stack[MAX_HEIGHT], int stack_top)
{
	// Empty tree
	if (stack_top == -1) {
		rbt->root = rbt_node_new(key, RED, data);
		return 1;
	}

	rbt_node_t *parent = node_stack[stack_top];
	if (key == parent->key)     return 0;
	else if (key < parent->key) parent->left = rbt_node_new(key, RED, data);
	else                        parent->right = rbt_node_new(key, RED, data);
	return 1;
}

static int _rbt_insert_helper(rbt_t *rbt, int key, void *data)
{
	rbt_node_t *node_stack[MAX_HEIGHT];
	int stack_top;

	_traverse_with_stack(rbt, key, node_stack, &stack_top);
	int ret = _insert(rbt, key, data, node_stack, stack_top);
	if (ret == 0) return 0;

	_insert_rebalance(rbt, key, node_stack, stack_top);
	return 1;
}

static int _delete(rbt_t *rbt, int key, rbt_node_t *node_stack[MAX_HEIGHT],
                   int *stack_top, color_t *deleted_node_color, int *succ_key)
{
	rbt_node_t *leaf, *parent, *curr, *original_node = NULL;

	*succ_key = key;

	// Empty tree
	if (*stack_top == -1) return 0;

	leaf = node_stack[*stack_top];
	if (key != leaf->key) return 0;

	// If node has two children, find successor
	if (leaf->left && leaf->right) {
		original_node = leaf;
		curr = leaf->right;
		node_stack[++(*stack_top)] = curr;
		while (curr->left) {
			curr = curr->left;
			node_stack[++(*stack_top)] = curr;
		}
	}

	leaf = node_stack[*stack_top];
	if (*stack_top == 0) {
		if (!leaf->left) rbt->root = leaf->right;
		else             rbt->root = leaf->left;
	} else {
		parent = node_stack[*stack_top - 1];
		if (key < parent->key) {
			if (!leaf->left) parent->left = leaf->right;
			else             parent->left = leaf->left;
		} else {
			if (!leaf->left) parent->right = leaf->right;
			else             parent->right = leaf->left;
		}
	}
	
	// Add the non-NULL child of leaf (or NULL, if leaf has 0 children)
	// in the stack. We do this for the rebalancing that follows.
	node_stack[*stack_top] = leaf->left;
	if (!leaf->left) node_stack[*stack_top] = leaf->right;

	if (original_node) {
		original_node->key = leaf->key;
		*succ_key = leaf->key;
	}

	*deleted_node_color = leaf->color;

	return 1;
}

static void _delete_rebalance(rbt_t *rbt, int key, 
                            rbt_node_t *node_stack[MAX_HEIGHT], int stack_top)
{
	rbt_node_t *curr, *parent, *gparent, *sibling;

	while (1) {
		curr = node_stack[stack_top--];
		if (IS_RED(curr)) {
			curr->color = BLACK;
			return;
		}
		if (stack_top == -1) {
			if (IS_RED(curr)) curr->color = BLACK;
			return;
		}

		parent = node_stack[stack_top];
		
		if (key < parent->key) { // `curr` is left child
			sibling = parent->right;
			if (IS_RED(sibling)) { // CASE 1
				sibling->color = BLACK;
				parent->color = RED;
				if (stack_top - 1 == -1) {
					rbt->root = rbt_rotate_left(parent);

					// CASES 2, 3 and/or 4 below need to get the right stack
					node_stack[0] = sibling;
					node_stack[1] = parent;
					stack_top = 1;
				} else {
					gparent = node_stack[stack_top - 1];
					if (key < gparent->key) gparent->left = rbt_rotate_left(parent);
					else                   gparent->right = rbt_rotate_left(parent);

					// CASES 2, 3 and/or 4 below need to get the right gparent
					node_stack[stack_top - 1] = sibling;
				}
				sibling = parent->right;
			}

			if (IS_BLACK(sibling->left) && IS_BLACK(sibling->right)) { // CASE 2
				sibling->color = RED;
				continue;
			} else {
				if (IS_BLACK(sibling->right)) { // CASE 4
					sibling->left->color = BLACK;
					sibling->color = RED;
					parent->right = rbt_rotate_right(sibling);
					sibling = parent->right;
				}
				// CASE 3
				sibling->color = parent->color;
				parent->color = BLACK;
				sibling->right->color = BLACK;
				if (stack_top - 1 == -1) {
					rbt->root = rbt_rotate_left(parent);
				} else {
					gparent = node_stack[stack_top - 1];
					if (key < gparent->key) gparent->left = rbt_rotate_left(parent);
					else                   gparent->right = rbt_rotate_left(parent);
				}
				break;
			}
		} else { // `curr` is right child
			sibling = parent->left;
			if (IS_RED(sibling)) { // CASE 1
				sibling->color = BLACK;
				parent->color = RED;
				if (stack_top - 1 == -1) {
					rbt->root = rbt_rotate_right(parent);

					// CASES 2, 3 and/or 4 below need to get the right stack
					node_stack[0] = sibling;
					node_stack[1] = parent;
					stack_top = 1;
				} else {
					gparent = node_stack[stack_top - 1];
					if (key < gparent->key) gparent->left = rbt_rotate_right(parent);
					else                   gparent->right = rbt_rotate_right(parent);

					// CASES 2, 3 and/or 4 below need to get the right gparent
					node_stack[stack_top - 1] = sibling;
				}
				sibling = parent->left;
			}

			if (IS_BLACK(sibling->left) && IS_BLACK(sibling->right)) { // CASE 2
				sibling->color = RED;
				continue;
			} else {
				if (IS_BLACK(sibling->left)) { // CASE 4
					sibling->right->color = BLACK;
					sibling->color = RED;
					parent->left = rbt_rotate_left(sibling);
					sibling = parent->left;
				}
				// CASE 3
				sibling->color = parent->color;
				parent->color = BLACK;
				sibling->left->color = BLACK;
				if (stack_top - 1 == -1) {
					rbt->root = rbt_rotate_right(parent);
				} else {
					gparent = node_stack[stack_top - 1];
					if (key < gparent->key) gparent->left = rbt_rotate_right(parent);
					else                   gparent->right = rbt_rotate_right(parent);
				}
				break;
			}
		}
	}
}

static int _rbt_delete_helper(rbt_t *rbt, int key)
{
	rbt_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	color_t deleted_node_color;
	int succ_key;

	_traverse_with_stack(rbt, key, node_stack, &stack_top);
	int ret = _delete(rbt, key, node_stack, &stack_top, &deleted_node_color,
	                  &succ_key);
	if (ret == 0) return 0;

	if (deleted_node_color == BLACK)
		_delete_rebalance(rbt, succ_key, node_stack, stack_top);

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
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		ret = _rbt_insert_helper(rbt, key, NULL);
		nodes_inserted += ret;
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
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_lookup_helper(rbt, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
	td_ext_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_insert_helper(rbt, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	td_ext_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_delete_helper(rbt, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	return ret;
}

int rbt_validate(void *rbt)
{
	int ret = 0;
	ret = _rbt_validate_helper(((rbt_t *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _rbt_warmup_helper((rbt_t *)rbt, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "links_bu_internal_no_sentinels_stack_rebalance";
}
