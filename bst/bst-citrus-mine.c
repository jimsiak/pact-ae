/**
 * An internal binary search tree.
 **/
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <limits.h> /* for INT_MAX */

#include "alloc.h"
#include "arch.h"

#include "urcu.h"

#include "clargs.h" /* To get clargs.num_threads */

typedef struct bst_node_s {
	int key;
	void *data;

	struct bst_node_s *right,
	                  *left;

	pthread_spinlock_t lock;

	char marked;
//	char padding[CACHE_LINE_SIZE - sizeof(int) - sizeof(void *) -
//	             2 * sizeof(struct node_s *)];
//} __attribute__((packed)) __attribute__((aligned(CACHE_LINE_SIZE))) bst_node_t;
//} __attribute__((packed)) bst_node_t;
} bst_node_t;

typedef struct {
	bst_node_t *root;
} bst_t;

static bst_node_t *bst_node_new(int key, void *data)
{
	bst_node_t *node;

	XMALLOC(node, 1);
	node->key = key;
	node->data = data;
	node->right = node->left = NULL;
	pthread_spin_init(&node->lock, PTHREAD_PROCESS_SHARED);
	node->marked = 0;
	return node;
}

static bst_t *_bst_new_helper()
{
	bst_t *bst;
	XMALLOC(bst, 1);
	bst->root = bst_node_new(INT_MAX, NULL);
	bst->root->left = bst_node_new(INT_MAX-1, NULL);
	return bst;
}

/**
 * Traverses the tree `bst` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 **/
static inline void _traverse(bst_t *bst, int key, bst_node_t **parent,
                                                 bst_node_t **leaf)
{
	*parent = bst->root->left;
	*leaf = bst->root->left->left;

	while (*leaf) {
		int leaf_key = (*leaf)->key;
		if (leaf_key == key)
			return;

		*parent = *leaf;
		*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}

static int _bst_lookup_helper(bst_t *bst, int key)
{
	bst_node_t *parent, *leaf;

	urcu_read_lock();
	_traverse(bst, key, &parent, &leaf);
	urcu_read_unlock();
	return (leaf != NULL);
}

int validate(bst_node_t *prev, bst_node_t *curr, int direction){
	int result;     
	result = !(prev->marked);
	if (direction == 0) result = result && (prev->left == curr);
	else                result = result && (prev->right == curr);
	if (curr != NULL)
		result = result && (!curr->marked);
	return result;
}

static inline int _traverse_with_direction(bst_t *bst, int key,
                                           bst_node_t **prev_p, 
                                           bst_node_t **curr_p) 
{
	bst_node_t *prev = bst->root, *curr = prev->left;
    int direction = 0;
	int ckey = curr->key;

	while (curr && ckey != key) {
		prev = curr;
		if (ckey > key) {
			curr = curr->left;
			direction = 0;
		} else {
			curr = curr->right;
			direction = 1;
		}

		if (curr) ckey = curr->key;
	}

	*prev_p = prev;
	*curr_p = curr;
	return direction;
}

static int _bst_insert_helper(bst_t *bst, int key, void *value)
{
	bst_node_t *prev, *curr, *new;
	int direction;

	while(1){
		urcu_read_lock();
		direction = _traverse_with_direction(bst, key, &prev, &curr);
		urcu_read_unlock();

		// Key already in the tree
		if (curr != NULL) return 0;

		pthread_spin_lock(&prev->lock);
		if(!validate(prev, curr, direction)) {
			pthread_spin_unlock(&prev->lock);
			continue;
		}

		new = bst_node_new(key, value); 
		if (direction == 0) prev->left = new;
		else                prev->right = new;

		pthread_spin_unlock(&prev->lock);
		return 1;
	}
}

static int _bst_insert_helper_warmup(bst_t *bst, int key, void *value)
{
	bst_node_t *parent, *leaf;

	_traverse(bst, key, &parent, &leaf);

	// Empty tree case
	if (!parent && !leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Key already in the tree.
	if (leaf)
		return 0;

	if (key < parent->key)
		parent->left = bst_node_new(key, value);
	else
		parent->right = bst_node_new(key, value);

	return 1;
}

static int _bst_delete_helper(bst_t *bst, int key)
{
	bst_node_t *prev, *curr;
	int direction;

    while(1){
		urcu_read_lock();    
		direction = _traverse_with_direction(bst, key, &prev, &curr);
		urcu_read_unlock();

		// Key not found
		if (!curr)
			return 0;

		pthread_spin_lock(&prev->lock);
		pthread_spin_lock(&curr->lock);
		if(!validate(prev, curr, direction)) {
			pthread_spin_unlock(&prev->lock);
			pthread_spin_unlock(&curr->lock);
			continue;
		}

		if (!curr->left) {
			curr->marked = 1;
			if (direction == 0) prev->left = curr->right;
			else                prev->right = curr->right;
			pthread_spin_unlock(&prev->lock);
			pthread_spin_unlock(&curr->lock);
			return 1;
		} else if (!curr->right) {
			curr->marked = 1;
			if (direction == 0) prev->left = curr->left;
			else                prev->right = curr->left;
			pthread_spin_unlock(&prev->lock);
			pthread_spin_unlock(&curr->lock);
			return 1;
		}

		// FIXME Ugly code below :-)
		bst_node_t *prevSucc = curr;
        bst_node_t *succ = curr->right; 
        
            bst_node_t *next = succ->left;
            while ( next!= NULL){
                prevSucc = succ;
                succ = next;
                next = next->left;
            }		
        int succDirection = 1; 
        if (prevSucc != curr){
			pthread_spin_lock(&prevSucc->lock);
            succDirection = 0;
        } 		
		pthread_spin_lock(&succ->lock);
        if (validate(prevSucc,succ, succDirection) && validate(succ,NULL, 0)){
            curr->marked=1;
            bst_node_t *new = bst_node_new(succ->key, succ->data);
            new->left=curr->left;
            new->right=curr->right;
			pthread_spin_lock(&new->lock);
			if (direction == 0) prev->left = new;
			else                prev->right = new;
            urcu_synchronize();
            succ->marked=1;            
			if (prevSucc == curr){
                new->right=succ->right;
            }
            else{
                prevSucc->left=succ->right;
            }
			pthread_spin_unlock(&prev->lock);
			pthread_spin_unlock(&new->lock);
			pthread_spin_unlock(&curr->lock);
            if (prevSucc != curr)
				pthread_spin_unlock(&prevSucc->lock);
			pthread_spin_unlock(&succ->lock);
            return 1; 
        }
		pthread_spin_unlock(&prev->lock);
		pthread_spin_unlock(&curr->lock);
        if (prevSucc != curr)
			pthread_spin_unlock(&prevSucc->lock);
		pthread_spin_unlock(&succ->lock);
    }
}

static inline int _bst_warmup_helper(bst_t *bst, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = _bst_insert_helper_warmup(bst, key, NULL);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

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
	if (left && left->key >= root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

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

	_bst_validate_rec(root->left->left, 0);

	check_bst = (bst_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
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
	initURCU(clargs.num_threads);
	return _bst_new_helper();
}

void *rbt_thread_data_new(int tid)
{
	urcu_register(tid);
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
	ret = _bst_lookup_helper(rbt, key);
	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = _bst_insert_helper(rbt, key, value);
	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	ret = _bst_delete_helper(rbt, key);
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
	return "bst-citrus-mine";
}
