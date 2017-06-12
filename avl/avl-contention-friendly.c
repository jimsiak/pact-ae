/**
 * The Contention-Friendly Relaxed AVL tree.
 * "The Contention-Friendly Tree"
 *     Tyler Crain, Vincent Gramoli, and Michel Raynal (EuroPar' 2013)
 * See also here for the equivalent java code: 
 *    https://github.com/gramoli/synchrobench/blob/master/java/src/trees/lockbased/LockBasedFriendlyTreeMap.java
 **/
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h> // sleep()

#include "alloc.h"
#include "arch.h"

typedef struct avl_node_s {
	int key;
	// 'nothing' is necessary to align 'left'.
	int nothing;
	void *data;

	struct avl_node_s *left,
	                  *right;

	pthread_spinlock_t lock;

	#define REM_BY_LEFT_ROT 10
	char del, // node logically removed
	     rem; // node physically removed

	int left_h,
	    right_h,
	    local_h;
} __attribute__((packed)) avl_node_t;

typedef struct {
	avl_node_t *root;
} avl_t;

#define NODE_LOCK_INIT(node) pthread_spin_init(&(node)->lock, PTHREAD_PROCESS_SHARED)
#define NODE_LOCK(node)   pthread_spin_lock(&(node)->lock)
#define NODE_UNLOCK(node) pthread_spin_unlock(&(node)->lock)

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )

static avl_node_t *avl_node_new(int key, void *data)
{
	avl_node_t *node;

	XMALLOC(node, 1);
	node->key = key;
	node->data = data;
	node->right = node->left = NULL;
	NODE_LOCK_INIT(node);
	node->del = node->rem = 0;
	node->left_h = node->right_h = node->local_h = 0;
	return node;
}

static avl_t *_avl_new_helper()
{
	avl_t *avl;

	XMALLOC(avl, 1);
	avl->root = avl_node_new(-1, NULL);

	return avl;
}

/*****************************************************************************/
/*          Beginning of asynchronous balancing functions                    */
/*****************************************************************************/
static inline int remove_node(avl_node_t *parent, int left_child)
{
	avl_node_t *n, *child;

	if (parent->rem) return 0;
	if (left_child) n = parent->left;
	else            n = parent->right;
	if (!n) return 0;

	NODE_LOCK(parent);
	NODE_LOCK(n);
	if (!n->del) {
		NODE_UNLOCK(n);
		NODE_UNLOCK(parent);
		return 0;
	}

	if ((child = n->left) != NULL) {
		if (n->right != NULL) {
			NODE_UNLOCK(n);
			NODE_UNLOCK(parent);
			return 0;
		}
	} else {
		child = n->right;
	}

	if (left_child) parent->left = child;
	else            parent->right = child;

	n->left = parent;
	n->right = parent;
	n->rem = 1;
	NODE_UNLOCK(n);
	NODE_UNLOCK(parent);

	//>	update_node_heights();
	if (left_child) parent->left_h = n->local_h - 1;
	else            parent->right_h = n->local_h - 1;
	parent->local_h = MAX(parent->left_h, parent->right_h) + 1;

	return 1;
}

static inline void propagate(avl_node_t *n)
{
	avl_node_t *left = n->left;
	avl_node_t *right = n->right;

	if (left)  n->left_h = left->local_h;
	else       n->left_h = 0; 
	if (right) n->right_h = right->local_h;
	else       n->right_h = 0; 
	n->local_h = MAX(n->left_h, n->right_h) + 1;
}

static inline void rotate_right(avl_node_t *parent, int left_child)
{
	avl_node_t *n, *l, *r, *lr;
	avl_node_t *new;

	if (parent->rem) return;
	if (left_child) n = parent->left;
	else            n = parent->right;
	if (!n) return;

	l = n->left;
	if (!l) return;

	NODE_LOCK(parent);
	NODE_LOCK(n);
	NODE_LOCK(l);

	lr = l->right;
	r = n->right;

	new = avl_node_new(n->key, n->data);
	new->del = n->del;
	new->rem = n->rem;
	new->left = lr;
	new->right = r;
	l->right = new;

	n->rem = 1;

	if (left_child) parent->left = l;
	else            parent->right = l;

	NODE_UNLOCK(l);
	NODE_UNLOCK(n);
	NODE_UNLOCK(parent);

	//> update_node_heights();
	propagate(new);
	l->right_h = new->local_h;
	l->local_h = MAX(l->left_h, l->right_h) + 1;
	if (left_child) parent->left_h = l->local_h;
	else            parent->right_h = l->local_h;
	parent->local_h = MAX(parent->left_h, parent->right_h) + 1;
	return;
}

static inline void rotate_left(avl_node_t *parent, int left_child)
{
	avl_node_t *n, *l, *r, *rl;
	avl_node_t *new;

	if (parent->rem) return;
	if (left_child) n = parent->left;
	else            n = parent->right;
	if (!n) return;

	r = n->right;
	if (!r) return;

	NODE_LOCK(parent);
	NODE_LOCK(n);
	NODE_LOCK(r);

	rl = r->left;
	l = n->left;

	new = avl_node_new(n->key, n->data);
	new->del = n->del;
	new->rem = n->rem;
	new->left = l;
	new->right = rl;
	r->left = new;

	n->rem = REM_BY_LEFT_ROT;

	if (left_child) parent->left = r;
	else            parent->right = r;

	NODE_UNLOCK(r);
	NODE_UNLOCK(n);
	NODE_UNLOCK(parent);

	//> update_node_heights();
	propagate(new);
	r->left_h = new->local_h;
	r->local_h = MAX(r->left_h, r->right_h) + 1;
	if (left_child) parent->left_h = r->local_h;
	else            parent->right_h = r->local_h;
	parent->local_h = MAX(parent->left_h, parent->right_h) + 1;
	return;
}

static void rebalance_node(avl_node_t *parent, avl_node_t *node, int left_child)
{
	int balance, balance2;
	avl_node_t *left, *right;

	balance = node->left_h - node->right_h;
	if (balance >= 2) {
		left = node->left;
		balance2 = left->left_h - left->right_h;
		if (balance2 >= 0) {         // LEFT-LEFT case
			rotate_right(parent, left_child);
		} else if (balance2 < 0) { // LEFT-RIGHT case
			rotate_left(node, 1);
			rotate_right(parent, left_child);
		}
	} else if (balance <= -2) {
		right = node->right;
		balance2 = right->left_h - right->right_h;
		if (balance2 < 0) {         // RIGHT-RIGHT case
			rotate_left(parent, left_child);
		} else if (balance2 >= 0) { // RIGHT-LEFT case
			rotate_right(node, 0);
			rotate_left(parent, left_child);
		}
	}
}

static void restructure_node(avl_t *avl, avl_node_t *parent, avl_node_t *node,
                             int left_child)
{
	if (!node) return;

	avl_node_t *left = node->left;
	avl_node_t *right = node->right;

	//> Remove node if needed
	if (!node->rem && node->del && (!left || !right) && node != avl->root)
		if (remove_node(parent, left_child))
			return;

	//> Restructure subtrees
	if (!node->rem) {
		restructure_node(avl, node, left, 1);
		restructure_node(avl, node, right, 0);
	}

	if (!node->rem && node != avl->root) {
		propagate(node);
		rebalance_node(parent, node, left_child);
	}
}

volatile int stop_maint_thread;
pthread_t maint_thread;
static void *background_struct_adaptation(void *arg)
{
	avl_t *avl = arg;
	int i = 0;

	while (!stop_maint_thread) {
		restructure_node(avl, avl->root, avl->root->right, 0);
	}

	//> Do some more restructure before exiting
	for (i=0; i < 20; i++)
		restructure_node(avl, avl->root, avl->root->right, 0);
	return NULL;
}

static void spawn_maintenance_thread(avl_t *avl)
{
	stop_maint_thread = 0;
	pthread_create(&maint_thread, NULL, background_struct_adaptation, avl);
}
static void stop_maintenance_thread()
{
	stop_maint_thread = 1;
	pthread_join(maint_thread, NULL);
}
/*****************************************************************************/
/*****************************************************************************/
/*****************************************************************************/

static inline avl_node_t *get_next(avl_node_t *node, int key)
{
	avl_node_t *next;
	char rem = node->rem;

	if (rem == REM_BY_LEFT_ROT) next = node->right;
	else if (rem)               next = node->left;
	else if (key < node->key)   next = node->left;
	else if (node->key == key)  next = NULL;
	else                        next = node->right;
	return next;
}

static int _avl_lookup_helper(avl_t *avl, int key)
{
	avl_node_t *curr, *next;
	curr = avl->root;
	while (1) {
		//> Here we don't call get_next() due to performance decrease
//		next = get_next(curr, key);
		if (key == curr->key) break;
		next = (key < curr->key) ? curr->left : curr->right;
		if (!next) break;
		curr = next;
	}
	if (curr->key == key && !curr->del) return 1;
	else                                return 0;
}

static int validate(avl_node_t *node, int key)
{
	avl_node_t *next;
	if (node->rem)             return 0;
	else if (key == node->key) return 1;
	else if (key < node->key) next = node->left;
	else                      next = node->right;

	if (!next) return 1;
	else       return 0;
}

static int _avl_insert_helper(avl_t *avl, int key, void *value)
{
	avl_node_t *curr, *next;
	int ret = 0;

	curr = avl->root;

	while (1) {
		next = get_next(curr, key);
		if (!next) {
			NODE_LOCK(curr);
			if (validate(curr, key)) break;
			NODE_UNLOCK(curr);
			return 0;
		} else {
			curr = next;
		}
	}

	if (key == curr->key) {
		if (curr->del) {
			curr->del = 0;
			ret = 1;
		}
	} else {
		if (key < curr->key) curr->left  = avl_node_new(key, value);
		else                 curr->right = avl_node_new(key, value);
		ret = 1;
	}

	NODE_UNLOCK(curr);
	return ret;
}

static int _avl_delete_helper(avl_t *avl, int key)
{
	avl_node_t *curr, *next;
	int ret = 0;

	curr = avl->root;

	while (1) {
		next = get_next(curr, key);
		if (!next) {
			NODE_LOCK(curr);
			if (validate(curr, key)) break;
			NODE_UNLOCK(curr);
			return 0;
		} else {
			curr = next;
		}
	}

	if (key == curr->key && !curr->rem && !curr->del) {
		curr->del = 1;
		ret = 1;
	}

	NODE_UNLOCK(curr);
	return ret;
}

static int _avl_insert_helper_warmup(avl_t *avl, int key, void *value)
{
	avl_node_t *curr, *next;
	curr = NULL;
	next = avl->root;
	while (next) {
		if (key == next->key) return 0;
		curr = next;
		if (key < next->key) next = next->left;
		else                 next = next->right;
	}
	if (!curr) avl->root = avl_node_new(key, value);
	else if (key < curr->key) curr->left = avl_node_new(key, value);
	else                      curr->right = avl_node_new(key, value);
	return 1;
}

static inline int _avl_warmup_helper(avl_t *avl, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = _avl_insert_helper_warmup(avl, key, NULL);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

static int total_paths, total_nodes, marked_nodes;
static int bst_violations, avl_violations;
static int min_path_len, max_path_len;
static void _avl_validate_rec(avl_node_t *root, int _th)
{
	if (!root)
		return;

	avl_node_t *left = root->left;
	avl_node_t *right = root->right;

	total_nodes++;
	_th++;
	if (root->del) marked_nodes++;

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

	/* AVL violation? */
	int balance = root->left_h - root->right_h;
	if (balance < -1 || balance > 1)
		avl_violations++;

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
	marked_nodes = 0;
	bst_violations = 0;
	avl_violations = 0;

	_avl_validate_rec(root->right, 0);
//	_avl_validate_rec(root, 0);

	check_bst = (bst_violations == 0);
	check_avl = (avl_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  AVL Violation: %s\n",
	       check_avl ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size (Total [Marked / Unmarked]): %8d [%8d / %8d]\n",
	              total_nodes, marked_nodes, total_nodes - marked_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst && check_avl;
}

/*********************    FOR DEBUGGING ONLY    *******************************/
static void bst_print_rec(avl_node_t *root, int level)
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

	if (!root->del)
		printf("%d [%d(%d-%d)]\n", root->key, root->local_h,
		                           root->left_h, root->right_h);
	else
		printf("{%d} [%d(%d-%d)]\n", root->key, root->local_h,
		                             root->left_h, root->right_h);

	bst_print_rec(root->left, level + 1);
}

static void bst_print_struct(avl_t *bst)
{
	if (bst->root->right == NULL)
		printf("[empty]");
	else
		bst_print_rec(bst->root->right, 0);
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
	ret = _avl_lookup_helper(rbt, key);
	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = _avl_insert_helper(rbt, key, value);
	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	ret = _avl_delete_helper(rbt, key);
	return ret;
}

int rbt_validate(void *rbt)
{
	stop_maintenance_thread();
	int ret = 0;
	ret = _avl_validate_helper(((avl_t *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _avl_warmup_helper((avl_t *)rbt, nr_nodes, max_key, seed, force);
	spawn_maintenance_thread(rbt);
//	sleep(10);
//	stop_maintenance_thread();
	return ret;
}

char *rbt_name()
{
	return "avl-contention-friendly";
}
