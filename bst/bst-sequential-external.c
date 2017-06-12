/**
 * An internal binary search tree.
 **/
#include <stdio.h>
#include <stdlib.h>

#include "alloc.h"
#include "arch.h"

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )

typedef struct bst_node_s {
	int key;
	void *data;

	struct bst_node_s *right,
	                  *left;

//	char padding[CACHE_LINE_SIZE - sizeof(int) - sizeof(void *) -
//	             2 * sizeof(struct node_s *)];
//} __attribute__((packed)) __attribute__((aligned(CACHE_LINE_SIZE))) bst_node_t;
} bst_node_t;

typedef struct {
	bst_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spinlock_t bst_lock;
#	endif
} bst_t;

static bst_node_t *bst_node_new(int key, void *data)
{
	bst_node_t *node;

	XMALLOC(node, 1);
	node->key = key;
	node->data = data;
	node->right = node->left = NULL;
	return node;
}

static bst_t *_bst_new_helper()
{
	bst_t *bst;

	XMALLOC(bst, 1);
	bst->root = NULL;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spin_init(&bst->bst_lock, PTHREAD_PROCESS_SHARED);
#	endif

	return bst;
}

/**
 * Traverses the tree `bst` as dictated by `key`.
 * When returning, `leaf` is either NULL (empty tree) or the last node in the
 * access path. `parent` is either leaf's parent (if `leaf` != NULL) or
 * NULL.
 **/
static inline void _traverse(bst_t *bst, int key, bst_node_t **gparent,
                                                  bst_node_t **parent,
                                                  bst_node_t **leaf)
{
	*gparent = NULL;
	*parent = NULL;
	*leaf = bst->root;

	if (*leaf == NULL)
		return;

	while (!IS_EXTERNAL_NODE(*leaf)) {
		int leaf_key = (*leaf)->key;

		*gparent = *parent;
		*parent = *leaf;
		*leaf = (key <= leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}

static int _bst_lookup_helper(bst_t *bst, int key)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);
	return (leaf && leaf->key == key);
}

static int _bst_insert_helper(bst_t *bst, int key, void *value)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree case
	if (!leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Key already in the tree.
	if (leaf->key == key)
		return 0;

	// Create new internal and leaf nodes.
	bst_node_t *new_internal = bst_node_new(key, NULL);
	if (key <= leaf->key) {
		new_internal->left = bst_node_new(key, value);
		new_internal->right = leaf;
	} else {
		new_internal->left = leaf;
		new_internal->right = bst_node_new(key, value);
		new_internal->key = leaf->key;
	}

	if (!parent)
		bst->root = new_internal;
	else if (key <= parent->key)
		parent->left = new_internal;
	else
		parent->right = new_internal;

	return 1;
}

static int _bst_delete_helper(bst_t *bst, int key)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree or key not in the tree.
	if (!leaf || leaf->key != key)
		return 0;

	// Only one node in the tree.
	if (!parent) {
		bst->root = NULL;
		return 1;
	}

	bst_node_t *sibling = (key <= parent->key) ? parent->right : parent->left;
	if (!gparent)
		bst->root = sibling;
	else if (key <= gparent->key)
		gparent->left = sibling;
	else
		gparent->right = sibling;

	return 1;
}

static inline int _bst_warmup_helper(bst_t *bst, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = _bst_insert_helper(bst, key, NULL);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

static int key_in_max_path, key_in_min_path;
static int total_paths, total_nodes, bst_violations;
static int min_path_len, max_path_len;
static void _bst_validate_rec(bst_node_t *root, int _th)
{
	if (!root)
		return;

	bst_node_t *left = root->left;
	bst_node_t *right = root->right;

	total_nodes++;
	_th++;

	/* BST violation? */
	if (left && left->key > root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* We found a path (a node with at least one NULL child). */
	if (IS_EXTERNAL_NODE(root)) {
		total_paths++;

		if (_th <= min_path_len){
			min_path_len = _th;
			key_in_min_path = root->key;
		}
		if (_th >= max_path_len){
			max_path_len = _th;
			key_in_max_path = root->key;
		}
	}

	/* Check subtrees. */
	if (left)
		_bst_validate_rec(left, _th);
	if (right)
		_bst_validate_rec(right, _th);
}

static inline int _bst_validate_helper(bst_node_t *root)
{
	int check_bst = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;

	_bst_validate_rec(root, 0);

	check_bst = (bst_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("  Key of min path: %d\n", key_in_min_path);
	printf("  Key of max path: %d\n", key_in_max_path);
	printf("\n");

	return check_bst;
}

/*********************    FOR DEBUGGING ONLY    *******************************/
static void bst_print_rec(bst_node_t *root, int level)
{
	int i;

	if (root)
		bst_print_rec(root->right, level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%d\n", root->key);

	bst_print_rec(root->left, level + 1);
}

static void bst_print_struct(bst_t *bst)
{
	if (bst->root == NULL)
		printf("[empty]");
	else
		bst_print_rec(bst->root, 0);
	printf("\n");
}
/******************************************************************************/

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(bst_node_t));
	return _bst_new_helper();
}

void *rbt_thread_data_new(int tid)
{
	return NULL;
}

void rbt_thread_data_print(void *thread_data)
{
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((bst_t *)rbt)->bst_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _bst_lookup_helper(rbt, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((bst_t *)rbt)->bst_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((bst_t *)rbt)->bst_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _bst_insert_helper(rbt, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((bst_t *)rbt)->bst_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((bst_t *)rbt)->bst_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _bst_delete_helper(rbt, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((bst_t *)rbt)->bst_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	return ret;
}

int rbt_validate(void *rbt)
{
	int ret = 0;
	ret = _bst_validate_helper(((bst_t *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _bst_warmup_helper((bst_t *)rbt, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "bst-sequential-external";
}
