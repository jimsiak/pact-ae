#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm.h"
#	if !defined(TX_NUM_RETRIES)
#		define TX_NUM_RETRIES 20
#	endif
#endif

#include <assert.h>
#include "arch.h"
#include "alloc.h"
#include "rbt_links_td_ext_thread_data.h"

#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )
#define IS_BLACK(node) ( !(node) || !(node)->is_red )
#define IS_RED(node) ( !IS_BLACK(node) )

typedef struct rbt_node {
	int is_red;
	int key;
	void *value;
	struct rbt_node *link[2];

	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) - 
	             2 * sizeof(struct rbt_node *)];
} rbt_node_t;

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

static int _rbt_lookup_helper(rbt_t *rbt, int key)
{
	int dir;
	rbt_node_t *curr;

	if (!rbt->root)
		return 0;

	curr = rbt->root;
	while (!IS_EXTERNAL_NODE(curr)) {
		dir = curr->key < key;
		curr = curr->link[dir];
	}

	int found = (curr->key == key);

	return found;
}

/* returns 0, 1 or 2 depending on the number of rotations performed. */
static char _insert_fix_violation(rbt_t *rbt, int key,
                                  rbt_node_t **node_stack, int top,
                                  td_ext_thread_data_t *tdata, int wroot_level)
{
	rbt_node_t *curr, *parent, *sibling, *gparent;
	int dir_from_parent, dir_from_gparent;

	top--; /* Ignore the red child. */

	while (top > 1) {
		curr = node_stack[top--];

		if (IS_BLACK(curr))
			goto out;

#		ifdef VERBOSE_STATISTICS
		tdata->restructures_at_level[wroot_level + top - 1]++;
#		endif

		parent = node_stack[top];
		dir_from_parent = parent->key < key;
		sibling = parent->link[!dir_from_parent];
		if (IS_RED(sibling)) {
			parent->is_red = 1;
			curr->is_red = 0;
			sibling->is_red = 0;
			top--;
		} else { /* IS_BLACK(sibling) */
			gparent = node_stack[top-1];
			dir_from_gparent = gparent->key < key;
			if (IS_RED(curr->link[dir_from_parent])) {
				parent->is_red = 1;
				curr->is_red = 0;
				gparent->link[dir_from_gparent] = 
				     rbt_rotate_single(parent, !dir_from_parent);
				if (parent == rbt->root)
					rbt->root = gparent->link[dir_from_gparent];
				return 1;
			} else {
				parent->is_red = 1;
				curr->link[!dir_from_parent]->is_red = 0;
				gparent->link[dir_from_gparent] = 
				     rbt_rotate_double(parent, !dir_from_parent);
				if (parent == rbt->root)
					rbt->root = gparent->link[dir_from_gparent];
				return 2;
			}
		}
	}

out:
	return 0;
}

static int replace_external_node(rbt_node_t *parent, rbt_node_t *external,
                                 rbt_node_t *new[2])
{
	/* Key already there. */
	if (external->key == new[0]->key)
		return 0;

	external->link[0] = new[0];
	external->link[1] = new[1];
	external->is_red = 1;
	external->link[0]->is_red = 0;
	external->link[1]->is_red = 0;

	if (external->key > new[0]->key) {
		external->link[1]->key = external->key;
		external->key = external->link[0]->key;
	} else {
		external->link[0]->key = external->key;
	}
	return 1;
}

#ifndef ACCESS_PATH_MAX_DEPTH
#	define ACCESS_PATH_MAX_DEPTH 0
#endif

static inline int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *node[2],
                                     td_ext_thread_data_t *tdata)
{
	rbt_node_t *wroot, *wroot_parent;
	rbt_node_t *curr, *left, *right, *next;
	rbt_node_t head = {0}; /* False tree root. */
	rbt_node_t *node_stack[40];
	int dir, rr_sequence, top;
	int level = 0; /* The level of window root. */

	if (!rbt->root) {
		rbt->root = node[0];
		rbt->root->is_red = 0;
		return 1;
	}

	if (IS_EXTERNAL_NODE(rbt->root))
		return replace_external_node(NULL, rbt->root, node);

	if (IS_RED(rbt->root)) {
#		ifdef VERBOSE_STATISTICS
		tdata->restructures_at_level[0]++;
#		endif
		rbt->root->is_red = 0;
	} else if (IS_RED(rbt->root->link[0]) && IS_RED(rbt->root->link[1])) {
#		ifdef VERBOSE_STATISTICS
		tdata->restructures_at_level[0]++;
#		endif
		rbt->root->link[0]->is_red = 0;
		rbt->root->link[1]->is_red = 0;
	}

	head.link[1] = rbt->root;
	wroot_parent = &head;
	wroot = rbt->root;

	while (1) {
		/* Window root should never be an external node. */
		assert(!IS_EXTERNAL_NODE(wroot));

		top = -1;
		node_stack[++top] = wroot_parent;
		node_stack[++top] = wroot;

		curr = wroot;
		rr_sequence = 0;

		while (1) {
			dir = curr->key < node[0]->key;
			curr = curr->link[dir];
			node_stack[++top] = curr;

			if (IS_EXTERNAL_NODE(curr)) {
				if (replace_external_node(node_stack[top-1], curr, node)) {
					_insert_fix_violation(rbt, node[0]->key, node_stack, top,
					                      tdata, level);
					return 1;
				}
				return 0;
			}

			if (IS_BLACK(curr) &&
			    (IS_BLACK(curr->link[0]) || IS_BLACK(curr->link[1])))
			{
				wroot_parent = node_stack[top-1];
				wroot = curr;
				level += top - 1;
				break;
			}

			if (IS_RED(curr->link[0]) && IS_RED(curr->link[1])) {
				rr_sequence++;
				if (rr_sequence >= ACCESS_PATH_MAX_DEPTH) {
					curr->is_red = 1;
					curr->link[0]->is_red = 0;
					curr->link[1]->is_red = 0;
					char ret = _insert_fix_violation(rbt, node[0]->key,
					                                 node_stack, top,
					                                 tdata, level);
		
					dir = curr->key < node[0]->key;
					wroot_parent = curr;
					wroot = curr->link[dir];
					level += top - ret;
					break;
				}
			}
		}

//		dir = wroot->key < node[0]->key;
//		curr = wroot->link[dir];
//
//		rr_sequence = 0;
//
//		while (rr_sequence < ACCESS_PATH_MAX_DEPTH) {
//			if (!IS_EXTERNAL_NODE(curr) && IS_BLACK(curr) &&
//			    IS_RED(curr->link[0]) && IS_RED(curr->link[1])) {
//
//				node_stack[++top] = curr;
//
//				/* Bypass the red-red children. */
//				dir = curr->key < node[0]->key;
//				curr = curr->link[dir];
//				node_stack[++top] = curr;
//
//				dir = curr->key < node[0]->key;
//				curr = curr->link[dir];
//
//				rr_sequence++;
//			} else if (IS_RED(curr)) {
//				node_stack[++top] = curr;
//				dir = curr->key < node[0]->key;
//				curr = curr->link[dir];
//			} else {
//				break;
//			}
//		}
//
//		node_stack[++top] = curr;
//
//		if (IS_EXTERNAL_NODE(curr)) {
//			if (replace_external_node(node_stack[top-1], curr, node)) {
//				_insert_fix_violation(rbt, node[0]->key, node_stack, top,
//				                      tdata, level);
//				return 1;
//			}
//			return 0;
//		}
//
//		left = curr->link[0];
//		right = curr->link[1];
//
//		if (IS_BLACK(curr)) {
//			if (IS_BLACK(left) || IS_BLACK(right)) {
//				wroot_parent = node_stack[top-1];
//				wroot = curr;
//				level += top - 1;
//				continue;
//			} else {
//#				ifdef VERBOSE_STATISTICS
//				tdata->restructures_at_level[level + top - 1]++;
//#				endif
//				curr->is_red = 1;
//				left->is_red = 0;
//				right->is_red = 0;
//				char ret = _insert_fix_violation(rbt, node[0]->key,
//				                                 node_stack, top, tdata, level);
//
//				dir = curr->key < node[0]->key;
//				wroot = curr->link[dir];
//				wroot_parent = curr;
//				level += top - ret;
//				continue;
//			}
//		}
//
//		/* (IS_RED(curr)) */
//		dir = curr->key < node[0]->key;
//		next = curr->link[dir];
//
//		node_stack[++top] = next;
//		if (IS_EXTERNAL_NODE(next)) {
//			if (replace_external_node(curr, next, node)) {
//				_insert_fix_violation(rbt, node[0]->key, node_stack, top,
//				                      tdata, level);
//				return 1;
//			}
//			return 0;
//		}
//
//		if (IS_BLACK(next->link[0]) || IS_BLACK(next->link[1])) {
//			wroot_parent = curr;
//			wroot = next;
//			level += top - 1;
//			continue;
//		} else {
//#			ifdef VERBOSE_STATISTICS
//			tdata->restructures_at_level[level + top - 1]++;
//#			endif
//
//			next->is_red = 1;
//			next->link[0]->is_red = 0;
//			next->link[1]->is_red = 0;
//			char ret = _insert_fix_violation(rbt, node[0]->key, node_stack, top,
//			                      tdata, level);
//
//			dir = next->key < node[0]->key;
//			wroot_parent = next;
//			wroot = next->link[dir];
//			level += top - ret;
//			continue;
//		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static int delete_external_node(rbt_t *rbt,
                                rbt_node_t *gparent, rbt_node_t *parent,
                                rbt_node_t *ext, int key,
                                rbt_node_t **nodes_to_delete)
{
	if (ext->key != key)
		return 0;

	int dir_from_gparent = gparent->key < key;
	int dir_from_parent = parent->key < key;

	nodes_to_delete[0] = ext;
	nodes_to_delete[1] = parent;
	gparent->link[dir_from_gparent] = parent->link[!dir_from_parent];
	if (parent == rbt->root)
		rbt->root = gparent->link[dir_from_gparent];

	return 1;
}

/**
 * Call it only when there is a short node in the tree.
 * Returns 0,1 or 2 depending on the rotations performed.
 **/
static char _delete_fix_violation(rbt_t *rbt, int key,
                                  rbt_node_t **node_stack, int top,
                                  td_ext_thread_data_t *tdata, int wroot_level)
{
	rbt_node_t *curr, *sibling, *parent, *gparent;
	int dir_from_parent, dir_from_gparent;

	if (IS_RED(node_stack[top])) {
		node_stack[top]->is_red = 0;
		return 0;
	}

	while (top > 0) {
		curr = node_stack[top--];
		if (IS_RED(curr)) {
			curr->is_red = 0;
			return 0;
		}

		parent = node_stack[top--];
		dir_from_parent = parent->key < key;
		sibling = parent->link[!dir_from_parent];

		if (IS_RED(sibling)) {
			/* Case 1: RED sibling, reduce to a BLACK sibling case. */
			parent->is_red = 1;
			sibling->is_red = 0;
			gparent = node_stack[top];

			dir_from_gparent = gparent->key < key;
			gparent->link[dir_from_gparent] = 
			            rbt_rotate_single(parent, dir_from_parent);
			if (rbt->root == parent)
				rbt->root = gparent->link[dir_from_gparent];

			node_stack[++top] = sibling;
			sibling = parent->link[!dir_from_parent];
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[wroot_level + top + 1]++;
#			endif
		}

		if (IS_BLACK(sibling->link[0]) && IS_BLACK(sibling->link[1])) {
			/* Case 2: BLACK sibling with two BLACK children. */
			sibling->is_red = 1;
			node_stack[++top] = parent; /* new curr, is the parent. */
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[wroot_level + top + 1]++;
#			endif
			continue;
		} else if (IS_RED(sibling->link[!dir_from_parent])) {
			/* Case 3: BLACK sibling with RED same direction child. */
			int parent_color = parent->is_red;

			gparent = node_stack[top];
			dir_from_gparent = gparent->key < key;
			gparent->link[dir_from_gparent] = 
			            rbt_rotate_single(parent, dir_from_parent);
			if (rbt->root == parent)
				rbt->root = gparent->link[dir_from_gparent];
			gparent->link[dir_from_gparent]->is_red = parent_color;
			gparent->link[dir_from_gparent]->link[0]->is_red = 0;
			gparent->link[dir_from_gparent]->link[1]->is_red = 0;
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[wroot_level + top + 1]++;
#			endif
			return 1;
		} else {
			/* Case 4: BLACK sibling with RED different direction child. */
			int parent_color = parent->is_red;

			gparent = node_stack[top];
			dir_from_gparent = gparent->key < key;
			gparent->link[dir_from_gparent] = 
			            rbt_rotate_double(parent, dir_from_parent);
			if (rbt->root == parent)
				rbt->root = gparent->link[dir_from_gparent];
			gparent->link[dir_from_gparent]->is_red = parent_color;
			gparent->link[dir_from_gparent]->link[0]->is_red = 0;
			gparent->link[dir_from_gparent]->link[1]->is_red = 0;
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[wroot_level + top + 1]++;
#			endif
			return 2;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t **nodes_to_delete,
                              td_ext_thread_data_t *tdata)
{
	rbt_node_t *node_stack[40];
	rbt_node_t *wroot, *wroot_parent, *curr, *left, *right,
	           *other, *next, *next_next, *next_other;
	rbt_node_t head = { 0 };
	int dir, i, j, top;
	int bb_sequence, deleted_node_is_red;
	int level = 0; /* The level of the window root. */

	if (!rbt->root)
		return 0;
	if (IS_EXTERNAL_NODE(rbt->root)) {
		if (rbt->root->key == key) {
			nodes_to_delete[0] = rbt->root;
			rbt->root = NULL;
			return 1;
		}
		return 0;
	}
	if (IS_BLACK(rbt->root) && IS_BLACK(rbt->root->link[0]) && 
	                           IS_BLACK(rbt->root->link[1])) {
		int recolor_root = 1;
		if ((!IS_EXTERNAL_NODE(rbt->root->link[0]) &&
		    (IS_RED(rbt->root->link[0]->link[0]) ||
		    IS_RED(rbt->root->link[0]->link[1]))) ||
		    (!IS_EXTERNAL_NODE(rbt->root->link[1]) &&
			(IS_RED(rbt->root->link[1]->link[0]) ||
			 IS_RED(rbt->root->link[1]->link[1]))))
			recolor_root = 0;

		if (recolor_root) {
			rbt->root->is_red = 1;
#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[0]++;
#			endif
		}
	}

	head.link[1] = rbt->root;
	wroot_parent = &head;
	wroot = rbt->root;

	while (1) {
		/* Window root should never be an external node. */
		assert(!IS_EXTERNAL_NODE(wroot));

		top = -1;
		node_stack[++top] = wroot_parent;
		node_stack[++top] = wroot;

		curr = wroot;
		bb_sequence = 0;

		while (1) {
			dir = curr->key < key;
			curr = curr->link[dir];
			node_stack[++top] = curr;

			if (IS_EXTERNAL_NODE(curr)) {
				deleted_node_is_red = node_stack[top-1]->is_red;
				if (delete_external_node(rbt, node_stack[top-2], node_stack[top-1],
				                         curr, key, nodes_to_delete)) {
					if (deleted_node_is_red == 0) {
						dir = node_stack[top-2]->key < key;
						node_stack[top-1] = node_stack[top-2]->link[dir];
						top--;
						_delete_fix_violation(rbt, key, node_stack, top,
						                      tdata, level);
					}
					return 1;
				}
				return 0;
			}

			/* If curr is RED or has one RED child or grandchild make it window root. */
			if (IS_RED(curr) || IS_RED(curr->link[0]) || IS_RED(curr->link[1]) ||
			    (!IS_EXTERNAL_NODE(curr->link[0]) &&
				 (IS_RED(curr->link[0]->link[0]) ||
			      IS_RED(curr->link[0]->link[1]))) ||
			    (!IS_EXTERNAL_NODE(curr->link[1]) &&
				 (IS_RED(curr->link[1]->link[0]) ||
			      IS_RED(curr->link[1]->link[1])))) {
				wroot_parent = node_stack[top-1];
				wroot = curr;
				level += top - 1;
				break;
			}

			if (IS_BLACK(curr) && IS_BLACK(curr->link[0]) && IS_BLACK(curr->link[1]) && 
			    (!IS_EXTERNAL_NODE(curr->link[0]) &&
				 IS_BLACK(curr->link[0]->link[0]) && 
			     IS_BLACK(curr->link[0]->link[1])) &&
			    (!IS_EXTERNAL_NODE(curr->link[1]) &&
				 IS_BLACK(curr->link[1]->link[0]) &&
			     IS_BLACK(curr->link[1]->link[1]))) {
				bb_sequence++;
				if (bb_sequence >= ACCESS_PATH_MAX_DEPTH) {
					top--;
					rbt_node_t *parent = node_stack[top];
					parent->link[0]->is_red = 1;
					parent->link[1]->is_red = 1;
					char ret = _delete_fix_violation(rbt, key, node_stack, top, tdata,
					                                 level);
				
					dir = next->key < key;
					wroot_parent = parent;
					wroot = curr;
					level += top + ret % 2; /* Even in double rotation curr moves one level
					                           down. */
					break;
				}
			}
		}

//		dir = wroot->key < key;
//		curr = wroot->link[dir];
//
//		bb_sequence = 0;
//		while (bb_sequence < ACCESS_PATH_MAX_DEPTH) {
//			if (IS_EXTERNAL_NODE(curr))
//				break;
//			if (IS_RED(curr))
//				break;
//
//			dir = curr->key < key;
//			next = curr->link[dir];
//			other = curr->link[!dir];
//
//			if (IS_EXTERNAL_NODE(next))
//				break;
//			if (IS_RED(next) || IS_RED(other))
//				break;
//
//			/* Check if RED grandchild exists. */
//			for (i=0; i < 2; i++)
//				for (j=0; j < 2; j++)
//					if (IS_RED(curr->link[i]->link[j]))
//						break;
//
//			/* If we reached here curr is BLACK with BLACK (grand)children. */
//			bb_sequence++;
//			if (bb_sequence >= ACCESS_PATH_MAX_DEPTH)
//				break;
//
//			node_stack[++top] = curr;
//			curr = next;
//		}
//
//		node_stack[++top] = curr;
//
//		if (IS_EXTERNAL_NODE(curr)) {
//			deleted_node_is_red = node_stack[top-1]->is_red;
//			if (delete_external_node(rbt, node_stack[top-2], node_stack[top-1],
//			                         curr, key, nodes_to_delete)) {
//				if (deleted_node_is_red == 0) {
//					dir = node_stack[top-2]->key < key;
//					node_stack[top-1] = node_stack[top-2]->link[dir];
//					top--;
//					_delete_fix_violation(rbt, key, node_stack, top,
//					                      tdata, level);
//				}
//				return 1;
//			}
//			return 0;
//		}
//
//		if (IS_RED(curr)) {
//			wroot_parent = node_stack[top-1];
//			wroot = curr;
//			level += top - 1;
//			continue;
//		}
//
//		dir = curr->key < key;
//		next = curr->link[dir];
//		other = curr->link[!dir];
//
//		if (IS_EXTERNAL_NODE(next)) {
//			deleted_node_is_red = curr->is_red;
//			if (delete_external_node(rbt, node_stack[top-1], curr, next, key,
//			                         nodes_to_delete)) {
//				if (deleted_node_is_red == 0) {
//					node_stack[top] = other;
//					_delete_fix_violation(rbt, key, node_stack, top,
//					                      tdata, level);
//				}
//				return 1;
//			}
//			return 0;
//		}
//
//		if (IS_RED(next) || IS_RED(next->link[0]) ||
//		                    IS_RED(next->link[1])) {
//			wroot_parent = curr;
//			wroot = next;
//			level += top;
//			continue;
//		}
//		if (IS_RED(other) || IS_RED(other->link[0]) ||
//		                     IS_RED(other->link[1])) {
//			wroot_parent = node_stack[top-1];
//			wroot = curr;
//			level += top - 1;
//			continue;
//		}
//
//#		ifdef VERBOSE_STATISTICS
//		tdata->restructures_at_level[level + top - 1]++;
//#		endif
//
//		other->is_red = 1;
//		next->is_red = 1;
//		char ret = _delete_fix_violation(rbt, key, node_stack, top, tdata,
//		                                 level);
//	
//		dir = next->key < key;
//		wroot_parent = curr;
//		wroot = next;
//		level += top + ret % 2; /* Even in double rotation curr moves one level
//		                           down. */
	}

	/* Unreachable */
	assert(0);
	return -1;
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
	td_ext_thread_data_t *data = td_ext_thread_data_new(tid);

#	if defined(SYNC_CG_HTM)
	data->priv = tx_thread_data_new(tid);
#	endif

	return data;
}

#if defined(STATS_LACQS_PER_LEVEL)
#	define MAX_LEVEL 40
	static unsigned long long lacqs_per_level[MAX_LEVEL];
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
		printf("  Level %3d: %10llu Lacqs\n", i, lacqs_per_level[i]);
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
	int ret;
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
	int tx_ret = tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	if defined(STATS_LACQS_PER_LEVEL)
	lacqs_per_level[ret] += tx_ret;
#	endif
#	endif

	return (ret != 0);
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
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

int rbt_delete(void *rbt, void *thread_data, int key)
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

	free(nodes_to_delete[0]);
	free(nodes_to_delete[1]);

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
	char *str;
	XMALLOC(str, 100);
	sprintf(str, "links_td_tarjan_external ( ACCESS_PATH_MAX_DEPTH: %d )\n",
	        ACCESS_PATH_MAX_DEPTH);
	return str;
}
