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

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )
#define IS_BLACK(node) ( !(node) || (!(node)->color == BLACK) )
#define IS_RED(node) ( !IS_BLACK(node) )

typedef enum {
	RED = 0,
	BLACK
} color_t;

typedef struct rbt_node {
	color_t color;
	int key;
	void *value;

	rbt_node_t *parent,
	           *left,
	           *right;


	char padding[CACHE_LINE_SIZE - sizeof(color_t) - sizeof(int) - 
	             3 * sizeof(rbt_node_t *) - sizeof(void *)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;

typedef struct {
	rbt_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spinlock_t rbt_lock;
#	endif
} rbt_t;

static rbt_node_t *rbt_node_new(int key, void *value)
{
	rbt_node_t *ret;
	
	XMALLOC(ret, 1);
	ret->color = RED;
	ret->key = key;
	ret->value = value;
	ret->parent = NULL;
	ret->left = NULL;
	ret->right = NULL;

	return ret;
}

static inline rbt_t *_rbt_new_helper()
{
	rbt_t *ret;

	XMALLOC(ret, 1);
	ret->root = NULL;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spin_init(&ret->rbt_lock, PTHREAD_PROCESS_SHARED);
#	endif

	return ret;
}

/* FIXME */

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
/* END OF FIXME */

/* FROM NOW ON NOT TOUCHED YET. */

//> Returns the level at which the lookup stopped.
static int _rbt_lookup_rec(rbt_node_t *root, int key, int level, int *found)
{
	//> Empty tree.
	if (!root)
		return level;

#	if defined(TEST_WRITING_LOOKUP)
	//> Just touch the node to add it to the transaction's write set.
	root->key = root->key;
#	endif

	//> Have we reached the desired external node?
	if (IS_EXTERNAL_NODE(root) && root->key == key) {
		*found = 1;
		return level;
	}

#	if defined(TEST_WRITING_LOOKUP)
	//> Also touch the children to maximize the write set.
	root->key = root->key;
	if (!IS_EXTERNAL_NODE(root)) {
		root->link[0]->key = root->link[0]->key;
		root->link[1]->key = root->link[1]->key;
//		int dir = root->key < key;
//		if (!IS_EXTERNAL_NODE(root->link[!dir])) {
//			root->link[!dir]->link[0]->key = root->link[!dir]->link[0]->key;
//			root->link[!dir]->link[1]->key = root->link[!dir]->link[1]->key;
//
//			int i;
//			for (i=0; i <= 1; i++) {
//				if (!IS_EXTERNAL_NODE(root->link[!dir]->link[i])) {
//					root->link[!dir]->link[i]->link[0]->key = root->link[!dir]->link[i]->link[0]->key;
//					root->link[!dir]->link[i]->link[1]->key = root->link[!dir]->link[i]->link[1]->key;
//				}
//			}
//		}
	}
#	endif

	int dir = root->key < key;
	return _rbt_lookup_rec(root->link[dir], key, level+1, found);
}

static rbt_node_t *_rbt_insert_rec(rbt_t *rbt, rbt_node_t *root, 
                                   rbt_node_t *node[2], int *found, int level)
{
	if (!root) {
		root = node[0];
		return root;
	}

	if (IS_EXTERNAL_NODE(root) && root->key == node[0]->key) {
		*found = 1;
//		printf("Found at level %2d\n", level);
		return root;
	}

	if (IS_EXTERNAL_NODE(root)) {
		root->link[0] = node[0];
		root->link[1] = node[1];
		root->is_red = 1;
		root->link[0]->is_red = 0;
		root->link[1]->is_red = 0;

		if (root->key > node[0]->key) {
			root->link[1]->key = root->key;
			root->key = root->link[0]->key;
		} else {
			root->link[0]->key = root->key;
		}

		return root;
	}

	int dir = root->key < node[0]->key;
	rbt_node_t *new_link_dir = _rbt_insert_rec(rbt, root->link[dir],
	                                           node, found, level + 1);

	if (root->link[dir] != new_link_dir) {
//		printf("HAHA\n");
		root->link[dir] = new_link_dir;
	}

	/* DEBUG */
	if (*found)
		return root;

	printf("LOOOOOOOL\n");

	/* If we caused a Red-Red violation let's fix it. */
	if (IS_BLACK(root->link[dir]))
		return root;

	if (IS_BLACK(root->link[dir]->link[0]) && 
	    IS_BLACK(root->link[dir]->link[1]))
		return root;

	/* root->link[dir] is red with one red child. */
	if (IS_RED(root->link[!dir])) {
		/* Case 1 */
		root->is_red = 1;
		root->link[dir]->is_red = 0;
		root->link[!dir]->is_red = 0;
	} else if (IS_BLACK(root->link[!dir])) {
		if (IS_RED(root->link[dir]->link[dir])) {
			/* Case 2 */
			root->is_red = 1;
			root->link[dir]->is_red = 0;
			root = rbt_rotate_single(root, !dir);
		} else {
			/* Case 3 */
			root->is_red = 1;
			root->link[dir]->link[!dir]->is_red = 0;
			root = rbt_rotate_double(root, !dir);
		}
	}

	return root;
}

static inline int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *nodes[2])
{
	int found = 0;

	rbt_node_t *new_root = _rbt_insert_rec(rbt, rbt->root, nodes,
	                                       &found, 0);
	if (rbt->root != new_root)
		rbt->root = new_root;
	if (IS_RED(rbt->root))
		rbt->root->is_red = 0;

	return !found;
}

static rbt_node_t *_rbt_delete_fixup(rbt_node_t *root, int dir, 
                                     int *done, int level)
{
	rbt_node_t *p = root;
	rbt_node_t *sibling;

	if (!root)
		return root;

	sibling = root->link[!dir];

	/* Case 1: RED sibling, reduce to a BLACK sibling case. */
	if (IS_RED(sibling)) {
		root->is_red = 1;
		sibling->is_red = 0;
		root = rbt_rotate_single(root, dir);
		sibling = p->link[!dir];
#		ifdef VERBOSE_STATISTICS
		rbt->verbose_stats->del_rotations_at_level[level]++;
#		endif
	}

	if (!sibling)
		return root;

	if (IS_BLACK(sibling->link[0]) && IS_BLACK(sibling->link[1])) {
		/* Case 2: BLACK sibling with two BLACK children. */
		if (IS_RED(p))
			*done = 1;
		p->is_red = 0;
		sibling->is_red = 1;
#		ifdef VERBOSE_STATISTICS
		rbt->verbose_stats->del_recolors_at_level[level]++;
#		endif
	} else {
		int parent_color = p->is_red;
		int new_root = (root == p);
		int rotation = 0;

		if (IS_RED(sibling->link[!dir])) {
			/* Case 3: BLACK sibling with RED same direction child. */
			p = rbt_rotate_single(p, dir);
			rotation = 1;
		} else {
			/* Case 4: BLACK sibling with RED different direction child. */
			p = rbt_rotate_double(p, dir);
			rotation = 2;
		}

#		ifdef VERBOSE_STATISTICS
		rbt->verbose_stats->del_rotations_at_level[level] += rotation;
#		endif

		p->is_red = parent_color;
		p->link[0]->is_red = 0;
		p->link[1]->is_red = 0;

		if (new_root)
			root = p;
		else
			root->link[dir] = p;

		*done = 1;
	}

	return root;
}

static rbt_node_t *_rbt_delete_rec(rbt_t *rbt, rbt_node_t *root, int key, 
                                   int *not_found, int *done, int level,
                                   rbt_node_t **node_to_delete)
{
	if (!root) {
		*done = 1;
		*not_found = 1;
		return root;
	}

	/* This should only happen when there is only one node in the tree. */
	if (IS_EXTERNAL_NODE(root) && root->key == key) {
		*node_to_delete = root;
		return NULL;
	}

	int dir;

	if (!IS_EXTERNAL_NODE(root)) {
		dir = root->link[1]->key == key;
		if (IS_EXTERNAL_NODE(root->link[dir]) && root->link[dir]->key == key) {
			rbt_node_t *save = root->link[!dir];
			if (IS_RED(root)) {
				*done = 1;
			} else if (IS_RED(save)) {
				save->is_red = 0;
				*done = 1;
			}

			*node_to_delete = root;
			return save;
		}
	}

	dir = root->key < key;
	rbt_node_t *new_link_dir = _rbt_delete_rec(rbt, root->link[dir], key,
	                                           not_found, done, level+1,
	                                           node_to_delete);
	if (root->link[dir] != new_link_dir)
		root->link[dir] = new_link_dir;

	if (!*done)
		root = _rbt_delete_fixup(root, dir, done, level);

	return root;
}

static inline int _rbt_delete_helper(rbt_t *rbt, int key, 
                                     rbt_node_t **node_to_delete)
{
	int done = 0, not_found = 0;
//	rbt_node_t *node_to_delete = NULL;

	rbt_node_t *new_root = _rbt_delete_rec(rbt, rbt->root, key, &not_found, 
	                                       &done, 0, node_to_delete);
	if (rbt->root != new_root)
		rbt->root = new_root;
	if (rbt->root && rbt->root->is_red)
		rbt->root->is_red = 0;

//	if (node_to_delete)
//		free(node_to_delete);

	return !not_found;
}

static int bh;
static int paths_with_bh_diff;
static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes, red_nodes, black_nodes;
static int red_red_violations, bst_violations;
static void _rbt_validate_rec(rbt_node_t *root, int _bh, int _th)
{
	if (!root)
		return;

	rbt_node_t *left = root->link[0];
	rbt_node_t *right = root->link[1];

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

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_rbt_validate_rec(left, _bh, _th);
	if (right)
		_rbt_validate_rec(right, _bh, _th);
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

	_rbt_validate_rec(root, 0, 0);

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

static rbt_node_t *_rbt_warmup_insert_rec(rbt_t *rbt, rbt_node_t *root, 
                                   rbt_node_t *node[2], int *found, int level)
{
	if (!root) {
		root = node[0];
		return root;
	}

	if (IS_EXTERNAL_NODE(root) && root->key == node[0]->key) {
		*found = 1;
		return root;
	}

	if (IS_EXTERNAL_NODE(root)) {
		root->link[0] = node[0];
		root->link[1] = node[1];
		root->is_red = 1;
		root->link[0]->is_red = 0;
		root->link[1]->is_red = 0;

		if (root->key > node[0]->key) {
			root->link[1]->key = root->key;
			root->key = root->link[0]->key;
		} else {
			root->link[0]->key = root->key;
		}

		return root;
	}

	int dir = root->key < node[0]->key;
	rbt_node_t *new_link_dir = _rbt_warmup_insert_rec(rbt, root->link[dir],
	                                           node, found, level + 1);

	if (root->link[dir] != new_link_dir)
		root->link[dir] = new_link_dir;

	/* DEBUG */
	if (*found)
		return root;

	/* If we caused a Red-Red violation let's fix it. */
	if (IS_BLACK(root->link[dir]))
		return root;

	if (IS_BLACK(root->link[dir]->link[0]) && 
	    IS_BLACK(root->link[dir]->link[1]))
		return root;

	/* root->link[dir] is red with one red child. */
	if (IS_RED(root->link[!dir])) {
		/* Case 1 */
		root->is_red = 1;
		root->link[dir]->is_red = 0;
		root->link[!dir]->is_red = 0;
	} else if (IS_BLACK(root->link[!dir])) {
		if (IS_RED(root->link[dir]->link[dir])) {
			/* Case 2 */
			root->is_red = 1;
			root->link[dir]->is_red = 0;
			root = rbt_rotate_single(root, !dir);
		} else {
			/* Case 3 */
			root->is_red = 1;
			root->link[dir]->link[!dir]->is_red = 0;
			root = rbt_rotate_double(root, !dir);
		}
	}

	return root;
}

static inline int _rbt_warmup_insert_helper(rbt_t *rbt, rbt_node_t *nodes[2])
{
	int found = 0;

	rbt_node_t *new_root = _rbt_warmup_insert_rec(rbt, rbt->root, nodes,
	                                       &found, 0);
	if (rbt->root != new_root)
		rbt->root = new_root;
	if (IS_RED(rbt->root))
		rbt->root->is_red = 0;

	return !found;
}

static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	rbt_node_t *nodes[2];
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		nodes[0] = rbt_node_new(key, NULL);
		nodes[1] = rbt_node_new(key, NULL);

		ret = _rbt_warmup_insert_helper(rbt, nodes);
		nodes_inserted += ret;

		if (!ret) {
			free(nodes[0]);
			free(nodes[1]);
		}
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
#	if defined(SYNC_CG_HTM)
	return tx_thread_data_new(tid);
#	else
	return NULL;
#	endif
}

#if defined(STATS_LACQS_PER_LEVEL)
#	define MAX_LEVEL 30
	static unsigned long long lacqs_per_level[MAX_LEVEL];
	static unsigned long long lookups_per_level[MAX_LEVEL];
	static unsigned long long aborts_per_level[MAX_LEVEL];
#endif

void rbt_thread_data_print(void *thread_data)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(thread_data);
#	endif

#	if defined(STATS_LACQS_PER_LEVEL)
	int i;
	for (i=0; i < MAX_LEVEL; i++)
		printf("  Level %3d: %10llu Lacqs out of %10llu lookups (%6.2lf%%) with %10llu aborts\n", i, 
		       lacqs_per_level[i], lookups_per_level[i], 
		       (double)lacqs_per_level[i] / lookups_per_level[i] * 100, 
		       aborts_per_level[i]);
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
	int ret;
	int found = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	int naborts = tx_start(TX_NUM_RETRIES, thread_data, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_lookup_rec(((rbt_t *)rbt)->root, key, 0, &found);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	int tx_ret = tx_end(thread_data, &((rbt_t *)rbt)->rbt_lock);
#	if defined(STATS_LACQS_PER_LEVEL)
	lookups_per_level[ret]++;
	lacqs_per_level[ret] += tx_ret;
	if (!tx_ret)
		aborts_per_level[ret] += naborts;
//	printf("Finished lookup of key %10d at level %3d with %2d aborts\n",
//	       key, ret, naborts);
#	endif
#	endif

	return found;
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret;
	rbt_node_t *nodes[2];

	nodes[0] = rbt_node_new(key, value);
	nodes[1] = rbt_node_new(key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((rbt_t *)rbt)->rbt_lock);
#	endif

//	ret = _rbt_insert_helper(rbt, key, value);
	ret = _rbt_insert_helper(rbt, nodes);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((rbt_t *)rbt)->rbt_lock);
#	endif

	if (!ret) {
		free(nodes[0]);
		free(nodes[1]);
	}

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret;
	rbt_node_t *node_to_delete = NULL;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_delete_helper(rbt, key, &node_to_delete);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((rbt_t *)rbt)->rbt_lock);
#	endif

	if (ret)
		free(node_to_delete);

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
	return "links_bu_external";
}
