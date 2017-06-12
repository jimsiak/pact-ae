/**
 * An internal AVL tree.
 **/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "alloc.h"
#include "arch.h"

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm.h"
#	define TX_NUM_RETRIES 50

//#	if !defined(TX_NUM_RETRIES)
//#		define TX_NUM_RETRIES 20
//#	endif
#endif

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )
#define MAX_HEIGHT 50

typedef struct avl_node_s {
	int key;
	void *data;

	int height;

	struct avl_node_s *right,
	                  *left;

//	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) -
//	             2 * sizeof(struct node_s *)];
//} __attribute__((packed)) __attribute__((aligned(CACHE_LINE_SIZE))) avl_node_t;
} __attribute__((packed)) avl_node_t;
//} __attribute__((aligned(CACHE_LINE_SIZE))) avl_node_t;

typedef struct {
	avl_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spinlock_t avl_lock;
#	endif
} avl_t;

static avl_node_t *avl_node_new(int key, void *data)
{
	avl_node_t *node;

	XMALLOC(node, 1);
	node->key = key;
	node->data = data;
	node->height = 0; // new nodes have height 0 and NULL has height -1.
	node->right = node->left = NULL;
	return node;
}

static inline int node_height(avl_node_t *n)
{
	if (!n)
		return -1;
	else
		return n->height;
}

static inline int node_balance(avl_node_t *n)
{
	if (!n)
		return 0;

	int hleft = (n->left) ? n->left->height : 0;
	int hright = (n->right) ? n->right->height : 0;

	return hleft - hright;
}

static avl_t *_avl_new_helper()
{
	avl_t *avl;

	XMALLOC(avl, 1);
	avl->root = NULL;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spin_init(&avl->avl_lock, PTHREAD_PROCESS_SHARED);
#	endif

	return avl;
}

static inline avl_node_t *rotate_right(avl_node_t *node)
{
	assert(node != NULL && node->left != NULL);

	avl_node_t *node_left = node->left;

	node->left = node->left->right;
	node_left->right = node;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_left->height = MAX(node_height(node_left->left), node_height(node_left->right)) + 1;
	return node_left;
}
static inline avl_node_t *rotate_left(avl_node_t *node)
{
	assert(node != NULL && node->right != NULL);

	avl_node_t *node_right = node->right;

	node->right = node->right->left;
	node_right->left = node;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_right->height = MAX(node_height(node_right->left), node_height(node_right->right)) + 1;
	return node_right;
}

/**
 * Traverses the tree `avl` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 * In the case of an empty tree both `parent` and `leaf` are NULL.
 **/
static inline void _traverse(avl_t *avl, int key, avl_node_t **parent,
                                                  avl_node_t **leaf)
{
	*parent = NULL;
	*leaf = avl->root;

	while (*leaf) {
		int leaf_key = (*leaf)->key;
		if (leaf_key == key)
			return;

		*parent = *leaf;
		*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}
static inline void _traverse_with_stack(avl_t *avl, int key,
                                        avl_node_t *node_stack[MAX_HEIGHT],
                                        int *stack_top)
{
	avl_node_t *parent, *leaf;

	parent = NULL;
	leaf = avl->root;
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

static int _avl_lookup_helper(avl_t *avl, int key)
{
	avl_node_t *parent, *leaf;

	_traverse(avl, key, &parent, &leaf);
	return (leaf != NULL);
}

static inline void _avl_insert_fixup(avl_t *avl, int key,
                                     avl_node_t *node_stack[MAX_HEIGHT],
                                     int top)
{
	avl_node_t *curr, *parent;

	while (top >= 0) {
		curr = node_stack[top--];

		parent = NULL;
		if (top >= 0) parent = node_stack[top];

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
	}
}

static int _avl_insert_helper(avl_t *avl, int key, void *value)
{
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;

	_traverse_with_stack(avl, key, node_stack, &stack_top);

	// Empty tree case
	if (stack_top < 0) {
		avl->root = avl_node_new(key, value);
		return 1;
	}

	avl_node_t *place = node_stack[stack_top];

	// Key already in the tree.
	if (place->key == key)
		return 0;

	if (key < place->key)
		place->left = avl_node_new(key, value);
	else
		place->right = avl_node_new(key, value);

	_avl_insert_fixup(avl, key, node_stack, stack_top);

	return 1;
}

static inline void _find_successor_with_stack(avl_node_t *node,
                                              avl_node_t *node_stack[MAX_HEIGHT],
                                              int *stack_top)
{
	avl_node_t *parent, *leaf;

	parent = node;
	leaf = node->right;
	node_stack[++(*stack_top)] = leaf;

	while (leaf->left) {
		parent = leaf;
		leaf = leaf->left;
		node_stack[++(*stack_top)] = leaf;
	}
}

static inline void _avl_delete_fixup(avl_t *avl, int key,
                                     avl_node_t *node_stack[MAX_HEIGHT],
                                     int top)
{
	avl_node_t *curr, *parent;

	while (top >= 0) {
		curr = node_stack[top--];

		parent = NULL;
		if (top >= 0) parent = node_stack[top];

		int balance = node_balance(curr);
		if (balance == 2) {
			int balance2 = node_balance(curr->left);

			if (balance2 == 0 || balance2 == 1) { // LEFT-LEFT case
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

			continue;
		} else if (balance == -2) {
			int balance2 = node_balance(curr->right);

			if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
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

			continue;
		}

		/* Update the height of current node. */
		int height_saved = node_height(curr);
		int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
		curr->height = height_new;
		if (height_saved == height_new)
			break;
	}

}

static int _avl_delete_helper(avl_t *avl, int key)
{
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;

	_traverse_with_stack(avl, key, node_stack, &stack_top);

	// Empty tree case
	if (stack_top < 0)
		return 0;

	avl_node_t *place = node_stack[stack_top];
	avl_node_t *parent = (stack_top-1 >= 0) ? node_stack[stack_top-1] : NULL;

	// Key not in the tree
	if (place->key != key)
		return 0;

	if (!place->left) {
		if (!parent) avl->root = place->right;
		else if (parent->left == place) parent->left = place->right;
		else if (parent->right == place) parent->right = place->right;
	} else if (!place->right) {
		if (!parent) avl->root = place->left;
		else if (parent->left == place) parent->left = place->left;
		else if (parent->right == place) parent->right = place->left;
	} else { // place has two children.
		avl_node_t *succ, *succ_parent;
		_find_successor_with_stack(place, node_stack, &stack_top);
		succ_parent = node_stack[stack_top-1];
		succ = node_stack[stack_top];

		place->key = succ->key;
		if (succ_parent->left == succ) succ_parent->left = succ->right;
		else succ_parent->right = succ->right;

		// This is necessary because if we call _avl_delete_fixup() with the
		// initial key then another path may be followed.
		key = place->key;
	}

	stack_top--;
	_avl_delete_fixup(avl, key, node_stack, stack_top);

	return 1;
}

static inline int _avl_warmup_helper(avl_t *avl, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = _avl_insert_helper(avl, key, NULL);
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
	if (left && left->key >= root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* AVL violation? */
	int balance = node_balance(root);
	if (balance < -1 || balance > 1)
		avl_violations++;

	/* We found a path (a node with at least one NULL child). */
	if (!left || !right) {
		total_paths++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_avl_validate_rec(left, _th);
	if (right)
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

	printf("%d\n", root->key);

	avl_print_rec(root->left, level + 1);
}

static void avl_print_struct(avl_t *avl)
{
	if (avl->root == NULL)
		printf("[empty]");
	else
		avl_print_rec(avl->root, 0);
	printf("\n");
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
#	if defined(SYNC_CG_HTM)
	return tx_thread_data_new(tid);
#	else
	return NULL;
#	endif
}

void rbt_thread_data_print(void *thread_data)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(thread_data);
#	endif
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_add(d1, d2, dst);
#	endif
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((avl_t *)rbt)->avl_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((avl_t *)rbt)->avl_lock);
#	endif

	ret = _avl_lookup_helper(rbt, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((avl_t *)rbt)->avl_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((avl_t *)rbt)->avl_lock);
#	endif

	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((avl_t *)rbt)->avl_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((avl_t *)rbt)->avl_lock);
#	endif

	ret = _avl_insert_helper(rbt, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((avl_t *)rbt)->avl_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((avl_t *)rbt)->avl_lock);
#	endif

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((avl_t *)rbt)->avl_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((avl_t *)rbt)->avl_lock);
#	endif

	ret = _avl_delete_helper(rbt, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((avl_t *)rbt)->avl_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((avl_t *)rbt)->avl_lock);
#	endif

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
	return "avl-sequential-internal";
}
