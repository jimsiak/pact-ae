#include <assert.h>
#include <string.h> /* strcmp() etc. */

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

#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )
#define IS_BLACK(node) ( !(node) || !(node)->is_red )
#define IS_RED(node) ( !IS_BLACK(node) )

typedef struct rbt_node {
	int is_red;
	char *key;
	void *value;
	struct rbt_node *link[2];

	char padding[CACHE_LINE_SIZE - sizeof(int) - sizeof(char *) - 
	             sizeof(void *) - 2 * sizeof(struct rbt_node *)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;

typedef struct {
	rbt_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spinlock_t rbt_lock;
#	endif
} rbt_t;

static rbt_node_t *rbt_node_new(char *key, void *value)
{
	rbt_node_t *ret;

	XMALLOC(ret, 1);
	ret->is_red = 1;
	ret->key = key;
	ret->value = value;
	ret->link[0] = NULL;
	ret->link[1] = NULL;

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

static rbt_node_t *rbt_rotate_single(rbt_node_t *root, int dir)
{
	rbt_node_t *save = root->link[!dir];

	root->link[!dir] = save->link[dir];
	save->link[dir] = root;

	return save;
}

static rbt_node_t *rbt_rotate_double(rbt_node_t *root, int dir)
{
	root->link[!dir] = rbt_rotate_single(root->link[!dir], !dir);
	return rbt_rotate_single(root, dir);
}

#define MAX_STR_LEN 256

/* Returns 0(= left) if (node->key >= key) else 1(= right). */
static inline int dir_next(rbt_node_t *node, char *key)
{
	int cmp = strncmp(node->key, key, MAX_STR_LEN);
	return ((cmp >= 0) ? 0 : 1);
}

//> Returns the level at which the lookup stopped.
static int _rbt_lookup_helper(rbt_t *rbt, char *key, int *found)
{
	int level = 0;
	rbt_node_t *curr = rbt->root;

	*found = 0;

	//> Empty tree.
	if (!curr)
		return level;

	while (!IS_EXTERNAL_NODE(curr)) {
#		if defined(TEST_WRITING_LOOKUP)
		//> Just touch the node to add it to the transaction's write set.
		curr->key = curr->key;
#		endif

//		int dir = curr->key < key;
		int dir = dir_next(curr, key);
		curr = curr->link[dir];
		level++;
	}

//	if (curr->key == key)
	if (strncmp(curr->key, key, MAX_STR_LEN) == 0)
		*found = 1;

	return level;
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
 **/
static inline void replace_external_node(rbt_node_t *root,
                                         rbt_node_t *nodes[2])
{
	root->link[0] = nodes[0];
	root->link[1] = nodes[1];
	root->is_red = 1;
	root->link[0]->is_red = 0;
	root->link[1]->is_red = 0;

//	if (root->key > nodes[0]->key) {
	if (strncmp(root->key, nodes[0]->key, MAX_STR_LEN) > 0) {
		root->link[1]->key = root->key;
		root->key = root->link[0]->key;
	} else {
		root->link[0]->key = root->key;
	}
}

/*
 * 'top' shows at the last occupied index of 'node_stack' which is the newly
 * added node to the tree and is RED.
 */
static inline void _rbt_insert_fixup(rbt_t *rbt, char *key,
                                     rbt_node_t *node_stack[100], int top,
                                     td_ext_thread_data_t *tdata)
{
#	ifdef VERBOSE_STATISTICS
	int nr_recolors = 0;
#	endif

	rbt_node_t *ggparent, *gparent, *parent, *uncle;

	assert(IS_RED(node_stack[top]));

	/* Consume the newly inserted RED node from the stack. */
	top--;

	while (top > 0) {
		parent = node_stack[top--];

		/* parent is BLACK, we are done. */
		if (IS_BLACK(parent))
			break;

		/* parent is RED so it cannot be root => it must have a parent. */
		gparent = node_stack[top--];

		/* What is the direction we followed from gparent to parent? */
//		int dir = gparent->key < key;
		int dir = dir_next(gparent, key);
		uncle = gparent->link[!dir];

		if (IS_RED(uncle)) {              /* Case 1 (Recolor and move up) */
			gparent->is_red = 1;
			parent->is_red = 0;
			uncle->is_red = 0;
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[top+2]++;
			nr_recolors++;
#			endif
			continue;
		} else {
			ggparent = (top >= 0) ? node_stack[top] : NULL;
//			int dir_from_parent = parent->key < key;
			int dir_from_parent = dir_next(parent, key);
			if (dir == dir_from_parent) { /* Case 2 (Single rotation) */
				gparent->is_red = 1;
				parent->is_red = 0;
				if (ggparent) {
//					int dir_from_ggparent = ggparent->key < key;
					int dir_from_ggparent = dir_next(ggparent, key);
					ggparent->link[dir_from_ggparent] = rbt_rotate_single(gparent, !dir);
				 } else {
					rbt->root = rbt_rotate_single(gparent, !dir);
				 }
			} else {                      /* Case 3 (Double rotation) */
				gparent->is_red = 1;
				parent->link[dir_from_parent]->is_red = 0;
				if (ggparent) {
//					int dir_from_ggparent = ggparent->key < key;
					int dir_from_ggparent = dir_next(ggparent, key);
					ggparent->link[dir_from_ggparent] = rbt_rotate_double(gparent, !dir);
				} else {
					rbt->root = rbt_rotate_double(gparent, !dir);
				}
			}
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[top+2]++;
#			endif
			break;
		}
	}

	if (IS_RED(rbt->root))
		rbt->root->is_red = 0;

#	ifdef VERBOSE_STATISTICS
	tdata->fixups_nr_recolors[nr_recolors]++;
#	endif
}

static inline int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *nodes[2],
                                     td_ext_thread_data_t *tdata)
{
	rbt_node_t *node_stack[100], *curr;
	int top = -1;

	/* Empty tree */
	if (!rbt->root) {
		rbt->root = nodes[0];
		rbt->root->is_red = 0;
		return 1;
	}

	/* Traverse the tree until an external node is reached. */
	curr = rbt->root;
	node_stack[++top] = curr;
	while (!IS_EXTERNAL_NODE(curr)) {
//		int dir = curr->key < nodes[0]->key;
		int dir = dir_next(curr, nodes[0]->key);
		curr = curr->link[dir];
		node_stack[++top] = curr;
	}

	/* Did we find the external node we were looking for? */
//	if (curr->key == nodes[0]->key)
	if (strncmp(curr->key, nodes[0]->key, MAX_STR_LEN) == 0)
		return 0;

	/* Insert the new node and fixup any violations. */
	replace_external_node(curr, nodes);
	_rbt_insert_fixup(rbt, nodes[0]->key, node_stack, top, tdata);

	return 1;
}

/*
 * The top of the stack contains the doubly-black node.
 */
static inline void _rbt_delete_fixup(rbt_t *rbt, char *key,
                                     rbt_node_t *node_stack[100], int top,
                                     td_ext_thread_data_t *tdata)
{
#	ifdef VERBOSE_STATISTICS
	int levels_up = 0;
#	endif

	rbt_node_t *curr, *sibling, *parent, *gparent;
	int dir_from_parent, dir_from_gparent;

	while (top > 0) {
		curr = node_stack[top--];
		if (IS_RED(curr)) {
			curr->is_red = 0;
#			ifdef VERBOSE_STATISTICS
			tdata->fixups_nr_recolors[levels_up]++;
#			endif
			return;
		}

		parent = node_stack[top--];
//		dir_from_parent = parent->key < key;
		dir_from_parent = dir_next(parent, key);
		sibling = parent->link[!dir_from_parent];

		if (IS_RED(sibling)) {
			/* Case 1: RED sibling, reduce to a BLACK sibling case. */
			parent->is_red = 1;
			sibling->is_red = 0;
			gparent = (top >= 0) ? node_stack[top] : NULL;
			if (gparent) {
//				dir_from_gparent = gparent->key < key;
				dir_from_gparent = dir_next(gparent, key);
				gparent->link[dir_from_gparent] = 
				            rbt_rotate_single(parent, dir_from_parent);
			} else {
				rbt->root = rbt_rotate_single(parent, dir_from_parent);
			}

#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[top+2]++;
#			endif

			node_stack[++top] = sibling;
			sibling = parent->link[!dir_from_parent];
		}

		if (IS_BLACK(sibling->link[0]) && IS_BLACK(sibling->link[1])) {
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[top+2]++;
			levels_up++;
#			endif

			/* Case 2: BLACK sibling with two BLACK children. */
			sibling->is_red = 1;
			node_stack[++top] = parent; /* new curr, is the parent. */
		} else if (IS_RED(sibling->link[!dir_from_parent])) {
			/* Case 3: BLACK sibling with RED same direction child. */
			int parent_color = parent->is_red;
			rbt_node_t *new_parent = NULL;

			gparent = (top >= 0) ? node_stack[top] : NULL;
			if (gparent) {
//				dir_from_gparent = gparent->key < key;
				dir_from_gparent = dir_next(gparent, key);
				gparent->link[dir_from_gparent] = 
				            rbt_rotate_single(parent, dir_from_parent);
				new_parent = gparent->link[dir_from_gparent];
			} else {
				rbt->root = rbt_rotate_single(parent, dir_from_parent);
				new_parent = rbt->root;
			}
			new_parent->is_red = parent_color;
			new_parent->link[0]->is_red = 0;
			new_parent->link[1]->is_red = 0;
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[top+2]++;
			tdata->fixups_nr_recolors[levels_up]++;
#			endif
			return;
		} else {
			/* Case 4: BLACK sibling with RED different direction child. */
			int parent_color = parent->is_red;
			rbt_node_t *new_parent = NULL;

			gparent = (top >= 0) ? node_stack[top] : NULL;
			if (gparent) {
//				dir_from_gparent = gparent->key < key;
				dir_from_gparent = dir_next(gparent, key);
				gparent->link[dir_from_gparent] = 
				            rbt_rotate_double(parent, dir_from_parent);
				new_parent = gparent->link[dir_from_gparent];
			} else {
				rbt->root = rbt_rotate_double(parent, dir_from_parent);
				new_parent = rbt->root;
			}
			new_parent->is_red = parent_color;
			new_parent->link[0]->is_red = 0;
			new_parent->link[1]->is_red = 0;
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[top+2]++;
			tdata->fixups_nr_recolors[levels_up]++;
#			endif
			return;
		}
	}

#	ifdef VERBOSE_STATISTICS
	tdata->fixups_nr_recolors[levels_up]++;
#	endif
}

static inline int _rbt_delete_helper(rbt_t *rbt, char *key, 
                                     rbt_node_t *nodes_to_delete[2],
                                     td_ext_thread_data_t *tdata)
{
	rbt_node_t *node_stack[100], *curr;
	rbt_node_t *parent, *gparent;
	int top = -1;

	/* Empty tree */
	if (!rbt->root) {
		return 0;
	}

	/* Traverse the tree until an external node is reached. */
	curr = rbt->root;
	node_stack[++top] = curr;
	while (!IS_EXTERNAL_NODE(curr)) {
//		int dir = curr->key < key;
		int dir = dir_next(curr, key);
		curr = curr->link[dir];
		node_stack[++top] = curr;
	}

	/* Key not in the tree. */
//	if (curr->key != key)
	if (strncmp(curr->key, key, MAX_STR_LEN) != 0)
		return 0;

	/* Key found. Delete the node and fixup any violations. */
	parent = (top >= 1) ? node_stack[top-1] : NULL;
	gparent = (top >= 2) ? node_stack[top-2] : NULL;

	if (!parent) { /* Only one node in the tree. */
		nodes_to_delete[0] = curr;
		nodes_to_delete[1] = NULL;
		rbt->root = NULL;
		return 1; /* No fixup necessary. */
	} else if (!gparent) { /* We don't have gparent so parent is the root. */
//		int dir_from_parent = parent->key < key;
		int dir_from_parent = dir_next(parent, key);

		rbt->root = parent->link[!dir_from_parent];
		nodes_to_delete[0] = parent;
		nodes_to_delete[1] = curr;
		return 1; /* No fixup necessary. */
	} else {
//		int dir_from_gparent = gparent->key < key;
		int dir_from_gparent = dir_next(gparent, key);
//		int dir_from_parent = parent->key < key;
		int dir_from_parent = dir_next(parent, key);

		gparent->link[dir_from_gparent] = parent->link[!dir_from_parent];
		nodes_to_delete[0] = parent;
		nodes_to_delete[1] = curr;

		/* If parent was BLACK we need to fixup.
		   First remove parent, curr from the stack and and curr's sibling.
		 */
		top--;
		node_stack[top] = parent->link[!dir_from_parent];

		if (IS_BLACK(parent))
			_rbt_delete_fixup(rbt, key, node_stack, top, tdata);

		return 1;
	}

	/* Unreachable */
	return 0;
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
//	if (left && left->key > root->key)
	if (left && strncmp(left->key, root->key, MAX_STR_LEN) > 0)
		bst_violations++;
//	if (right && right->key <= root->key)
	if (right && strncmp(right->key, root->key, MAX_STR_LEN) <= 0)
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

//	if (IS_EXTERNAL_NODE(root) && root->key == node[0]->key) {
	if (IS_EXTERNAL_NODE(root) &&
	    strncmp(root->key, node[0]->key, MAX_STR_LEN) == 0) {
		*found = 1;
		return root;
	}

	if (IS_EXTERNAL_NODE(root)) {
//		root->link[0] = node[0];
//		root->link[1] = node[1];
//		root->is_red = 1;
//		root->link[0]->is_red = 0;
//		root->link[1]->is_red = 0;
//
//		if (root->key > node[0]->key) {
//			root->link[1]->key = root->key;
//			root->key = root->link[0]->key;
//		} else {
//			root->link[0]->key = root->key;
//		}
		replace_external_node(root, node);

		return root;
	}

//	int dir = root->key < node[0]->key;
	int dir = dir_next(root, node[0]->key);
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

#include <stdio.h>
#include <unistd.h>
static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	rbt_node_t *nodes[2];
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int int_key = rand() % max_key;
		char *key;
		XMALLOC(key, MAX_STR_LEN);
//		fscanf(STDIN_FILENO, "%s", key);
//		for (i=0; i < 900; i++)
//			key[i] = 'a';
		sprintf(key, "%030d", int_key);
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

/*  */
/*********************    FOR DEBUGGING ONLY    *******************************/
static void rbt_print_rec(rbt_node_t *root, int level)
{
	int i;

	if (root)
		rbt_print_rec(root->link[1], level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%s[%s]\n", root->key, root->is_red ? "RED" : "BLA");

	rbt_print_rec(root->link[0], level + 1);
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

#if defined(STATS_LACQS_PER_LEVEL)
#	define MAX_LEVEL 30
	static unsigned long long lacqs_per_level[MAX_LEVEL];
	static unsigned long long lookups_per_level[MAX_LEVEL];
	static unsigned long long aborts_per_level[MAX_LEVEL];
#endif

void rbt_thread_data_print(void *thread_data)
{
	td_ext_thread_data_t *tdata = thread_data;

	td_ext_thread_data_print(tdata);

#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(tdata->priv);
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
	td_ext_thread_data_t *_d1 = d1, *_d2 = d2, *_dst = dst;

	td_ext_thread_data_add(_d1, _d2, _dst);
#	if defined(SYNC_CG_HTM)
	tx_thread_data_add(_d1->priv, _d2->priv, _dst->priv);
#	endif
}

int rbt_lookup(void *rbt, void *thread_data, char *key)
{
	int ret;
	int found = 0;
	td_ext_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	int naborts = tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_lookup_helper(rbt, key, &found);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	int tx_ret = tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	if defined(STATS_LACQS_PER_LEVEL)
	lookups_per_level[ret]++;
	lacqs_per_level[ret] += tx_ret;
	if (!tx_ret)
		aborts_per_level[ret] += naborts;
#	endif
#	endif

	return found;
}

int rbt_insert(void *rbt, void *thread_data, char *key, void *value)
{
	int ret;
	rbt_node_t *nodes[2];
	td_ext_thread_data_t *tdata = thread_data;

	nodes[0] = rbt_node_new(key, value);
	nodes[1] = rbt_node_new(key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

//	ret = _rbt_insert_helper(rbt, key, value);
	ret = _rbt_insert_helper(rbt, nodes, thread_data);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	if (!ret) {
		free(nodes[0]);
		free(nodes[1]);
	}

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, char *key)
{
	int ret;
	rbt_node_t *nodes_to_delete[2] = {NULL, NULL};
	td_ext_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_delete_helper(rbt, key, nodes_to_delete, thread_data);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	if (ret) {
		free(nodes_to_delete[0]);
		free(nodes_to_delete[1]);
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
	return "links_bu_external_string_keys";
}

//int main()
//{
//	void *rbt = rbt_new();
//	rbt_warmup(rbt, 10, 99999, 0, 0);
//	rbt_print_struct(rbt);
//	rbt_validate(rbt);
//	return 0;
//}
