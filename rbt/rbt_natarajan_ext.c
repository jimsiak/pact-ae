//#include "arch.h"
//#include "alloc.h"
//#include "rbt_links_td_ext_thread_data.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h> /* uintptr_t */

#include "rbt_natarajan_ext.h"

/*********************    FOR DEBUGGING ONLY    *******************************/
static void rbt_print_rec(pnode_t *p_root, int level)
{
	int i;
	dnode_t *d_root;

	if (p_root) {
		d_root = PNODE_TO_DNODE(p_root);
		rbt_print_rec(d_root->link[1], level + 1);
	}

	for (i = 0; i < level; i++)
		printf("|--");

	if (!p_root) {
		printf("NULL\n");
		return;
	}

	d_root = PNODE_TO_DNODE(p_root);
	printf("%d[%s]\n", d_root->key, d_root->is_red ? "RED" : "BLA");

	rbt_print_rec(d_root->link[0], level + 1);
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

/**
 * Replace an external node with a RED internal with two children.
 * If key is equal to dnode->key 0 is returned and no replacement occurs.
 * Example (* means red node);
 *
 *       8                4*
 *     /   \     =>     /    \
 *   NULL NULL         4      8
 *                   /   \  /   \
 *                 NULL  NULL  NULL
 **/
static int replace_external_node(dnode_t *dnode, int key, void *value)
{
	dnode_t *dnode_left, *dnode_right;

	if (key == dnode->key) {
		return 0;
	} else	if (key < dnode->key) {
		dnode_left = dnode_new(key, value, 0);
		dnode_right = dnode_new(dnode->key, dnode->value, 0);
		dnode->key = key;
	} else {
		dnode_left = dnode_new(dnode->key, dnode->value, 0);
		dnode_right = dnode_new(key, value, 0);
	}

	dnode->is_red = 1;
	dnode->value = NULL;
	dnode->link[0] = pnode_new(dnode_left, FREE);
	dnode->link[1] = pnode_new(dnode_right, FREE);
	return 1;
}

static void fix_rr_violation(rbt_t *rbt, pnode_t *pwroot, pnode_t *pcurr, 
                             pnode_t *pcurr_next, int key)
{
	dnode_t *dwroot, *dcurr, *dcurr_next;

	dwroot = PNODE_TO_DNODE(pwroot);
	dcurr  = PNODE_TO_DNODE(pcurr);
	dcurr_next = PNODE_TO_DNODE(pcurr_next);

	/* RED-RED violation between dcurr and dcurr_next */
	int dcurr_dir = (key > dwroot->key);
	int dcurr_next_dir = (key > dcurr->key);

	if (dcurr_dir == dcurr_next_dir) {
		dwroot->is_red = 1;
		dcurr->is_red = 0;
		if (dcurr_dir == 0)
			rbt_rotate_right(rbt, pwroot);
		else
			rbt_rotate_left(rbt, pwroot);
	} else {
		dwroot->color = RED;
		dcurr_next->color = BLACK;	
		if (dcurr_dir == 0) {
			rbt_rotate_left(rbt, pcurr);
			rbt_rotate_right(rbt, pwroot);
		} else {
			rbt_rotate_right(rbt, pcurr);
			rbt_rotate_left(rbt, pwroot);
		}
	}

}



/**
 * Returns 0 if key is already there, 1 otherwise.
 **/
static int rbt_td_insert_serial(rbt_t *rbt, int key, void *value)
{
	pnode_t *pwroot, *pcurr; /* Window root and current node examining. */
	dnode_t *dwroot, *dcurr, *dleft, *dright;

	/* Empty tree */
	if (rbt->root == NULL) {
		rbt->root = pnode_new(dnode_new(key, value, 0), FREE);
		return 1;
	}

	pwroot = rbt->root;
	dwroot = PNODE_TO_DNODE(pwroot);
	if (IS_EXTERNAL_DNODE(dwroot))
		return replace_external_node(dwroot, key, value);

	/* If root is RED or has both children RED fix the situation. */
	dleft = PNODE_TO_DNODE(dwroot->link[0]);
	dright = PNODE_TO_DNODE(dwroot->link[1]);
	if (IS_RED(dwroot)) {
		dwroot->is_red = 0;
	} else if (IS_RED(dleft) && IS_RED(dright)) {
		dwroot->is_red = 0;
		dleft->is_red = 0;
		dright->is_red = 0;
	}

	while (1) {
		/* Window root should never be an external node. */
		if (IS_EXTERNAL_DNODE(dwroot)) {
			printf("Error: window root is external node!\n");
			exit(1);
		}

		pcurr = NEXT_CHILD_PNODE(dwroot, key);
		dcurr = PNODE_TO_DNODE(pcurr);

		/* Case 'a' of Tarjan's paper. */
		if (IS_EXTERNAL_DNODE(dcurr))
			return replace_external_node(dcurr, key, value);

		dleft = PNODE_TO_DNODE(dcurr->link[0]);
		dright = PNODE_TO_DNODE(dcurr->link[1]);

		if (IS_BLACK(dcurr)) {
			if (IS_BLACK(dleft) || IS_BLACK(dright)) {
				/* Case 'b' of Tarjan's paper. */
				pwroot = pcurr;
				dwroot = PNODE_TO_DNODE(pwroot);
				continue;
			} else {
				/* Case 'c' of Tarjan's paper. */
				/* Both dcurr children are RED. */
				dcurr->is_red = 1;
				dleft->is_red = 0;
				dright->is_red = 0;
				
				/* Red nodes are not external so no need to check here. */
				pwroot = NEXT_CHILD_PNODE(dcurr, key);
				dwroot = PNODE_TO_DNODE(pwroot);
				continue;
			}
		} else if (IS_RED(dcurr)) {
			pnode_t *pcurr_next = NEXT_CHILD_PNODE(dcurr, key);
			dnode_t *dcurr_next = PNODE_TO_DNODE(pcurr_next);

			if (IS_EXTERNAL_DNODE(dcurr_next)) {
				if (replace_external_node(dcurr_next, key, value)) {
					fix_rr_violation(rbt, pwroot, pcurr, pcurr_next, 
					                 key);
					return 1;
				} else {
					return 0;
				}
			}

			dnode_t *dcurr_next_left = PNODE_TO_DNODE(dcurr_next->link[0]);
			dnode_t *dcurr_next_right = PNODE_TO_DNODE(dcurr_next->link[1]);
			if (IS_BLACK(dcurr_next_left) || IS_BLACK(dcurr_next_right)) {
				pwroot = pcurr_next;
				dwroot = PNODE_TO_DNODE(pwroot);
				continue;
			} else {
				dcurr_next->is_red = 1;
				dcurr_next_left->is_red = 0;
				dcurr_next_right->is_red = 0;
				fix_rr_violation(rbt, pwroot, pcurr, pcurr_next, key);

				pwroot = NEXT_CHILD_PNODE(dcurr_next, key);
				dwroot = PNODE_TO_DNODE(pwroot);
				continue;
			}
		}
	}
	
	return 0;
}


/* For testing purposes... */
int main()
{
	rbt_t rbt;

	rbt_print_struct(&rbt);

	return 0;
}

//typedef struct rbt_node {
//	int is_red;
//	int key;
//	void *value;
//	struct rbt_node *link[2];
//
//	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) - 
//	             2 * sizeof(struct rbt_node *)];
//} rbt_node_t;
//
//typedef struct {
//	rbt_node_t *root;
//} rbt_t;
//
//static rbt_node_t *rbt_node_new(int key, void *value)
//{
//	rbt_node_t *ret;
//
//	XMALLOC(ret, 1);
//	ret->is_red = 1;
//	ret->key = key;
//	ret->value = value;
//	ret->link[0] = NULL;
//	ret->link[1] = NULL;
//
//	return ret;
//}
//
//static inline rbt_t *_rbt_new_helper()
//{
//	rbt_t *ret;
//
//	XMALLOC(ret, 1);
//	ret->root = NULL;
//
//	return ret;
//}
//
//static rbt_node_t *rbt_rotate_single(rbt_node_t *root, int dir)
//{
//	rbt_node_t *save = root->link[!dir];
//
//	root->link[!dir] = save->link[dir];
//	save->link[dir] = root;
//
//	return save;
//}
//
//static rbt_node_t *rbt_rotate_double(rbt_node_t *root, int dir)
//{
//	root->link[!dir] = rbt_rotate_single(root->link[!dir], !dir);
//	return rbt_rotate_single(root, dir);
//}
//
//static int _rbt_lookup_helper(rbt_t *rbt, int key)
//{
//	int dir;
//	rbt_node_t *curr;
//
//	if (!rbt->root)
//		return 0;
//
//	curr = rbt->root;
//	while (!IS_EXTERNAL_NODE(curr)) {
//		dir = curr->key < key;
//		curr = curr->link[dir];
//	}
//
//	int found = (curr->key == key);
//
//	return found;
//}
//
//static inline int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *node[2],
//                                     td_ext_thread_data_t *tdata)
//{
//	if (!rbt->root) {
//		rbt->root = node[0];
//		rbt->root->is_red = 0;
//		return 1;
//	}
//
//	/* gg = grandgrandparent, g = grandparent, p = parent, q = current. */
//	rbt_node_t *gg, *g, *p, *q;
//	rbt_node_t head = {0}; /* False tree root. */
//	int dir = 0, last;
//	int inserted = 0;
//#	ifdef VERBOSE_STATISTICS
//	int level = 0;
//#	endif
//
//	gg = &head;
//	g = p = NULL;
//	q = gg->link[1] = rbt->root;
//
//	/* Search down the tree */
//	while (1) {
//		if (IS_EXTERNAL_NODE(q)) {
//			if (q->key == node[0]->key)
//				break;
//
//			/* Insert new node at the bottom */
//			q->link[0] = node[0];
//			q->link[1] = node[1];
//			q->is_red = 1;
//			q->link[0]->is_red = 0;
//			q->link[1]->is_red = 0;
//
//			if (q->key > node[0]->key) {
//				q->link[1]->key = q->key;
//				q->key = q->link[0]->key;
//			} else {
//				q->link[0]->key = q->key;
//			}
//			inserted = 1;
//		} else if (IS_RED(q->link[0]) && IS_RED(q->link[1])) {
//			/* Color flip */
//			q->is_red = 1;
//			q->link[0]->is_red = 0;
//			q->link[1]->is_red = 0;
//
//#			ifdef VERBOSE_STATISTICS
//			tdata->restructures_at_level[level]++;
//#			endif
//		}
//		
//		/* Fix red violation */
//		if (IS_RED(q) && IS_RED(p)) {
//			int dir2 = (gg->link[1] == g);
//			
//			g->is_red = 1;
//			if (q == p->link[last]) {
//				p->is_red = 0;
//				gg->link[dir2] = rbt_rotate_single(g, !last);
//
//				last = dir;
//				dir = q->key < node[0]->key;
//				g = p;
//				p = q;
//				q = p->link[dir];
//
//#				ifdef VERBOSE_STATISTICS
//				tdata->restructures_at_level[level]++;
//#				endif
//
//				continue;
//			} else {
//				q->is_red = 0;
//				gg->link[dir2] = rbt_rotate_double(g, !last);
//
//				last = q->key < node[0]->key;
//				dir = q->link[last]->key < node[0]->key;
//				g = q;
//				p = g->link[last];
//				q = p->link[dir];
//
//#				ifdef VERBOSE_STATISTICS
//				tdata->restructures_at_level[level]++;
//#				endif
//
//				continue;
//			}
//		}
//
//		last = dir;
//		dir = q->key < node[0]->key;
//		
//		/* Update helpers */
//		if (g)
//			gg = g;
//		g = p;
//		p = q;
//		q = q->link[dir];
//
//#		ifdef VERBOSE_STATISTICS
//		tdata->passed_from_level[level]++;
//		level++;
//#		endif
//	}
//
//	/* Update root and make it BLACK */
//	if (rbt->root != head.link[1])
//		rbt->root = head.link[1];
//	if (IS_RED(rbt->root))
//		rbt->root->is_red = 0;
//
//	return inserted;
//}
//
//static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t **node_to_delete,
//                              td_ext_thread_data_t *tdata)
//{
//	if (!rbt->root)
//		return 0;
//
//	/* g = grandparent, p = parent, q = current */
//	rbt_node_t *g, *p, *q;
//	rbt_node_t *f = NULL;
//	rbt_node_t head = { 0 };
//	int dir = 1;
//	int ret = 0;
//#	ifdef VERBOSE_STATISTICS
//	int level = -1;
//#	endif
//
//	/* Set up helpers */
//	q = &head;
//	g = p = NULL;
//	q->link[1] = rbt->root;
//
//	while (!IS_EXTERNAL_NODE(q)) {
//		int last = dir;
//
//		/* Update helpers. */
//		g = p;
//		p = q;
//		q = q->link[dir];
//		dir = q->key < key;
//#		ifdef VERBOSE_STATISTICS
//		level++;
//		tdata->passed_from_level[level]++;
//#		endif
//
//		if (IS_EXTERNAL_NODE(q))
//			break;
//
//		if (IS_BLACK(q) && IS_BLACK(q->link[dir])) {
//			if (IS_RED(q->link[!dir])) {
//				q->is_red = 1;
//				q->link[!dir]->is_red = 0;
//				p = p->link[last] = rbt_rotate_single(q, dir);
//#				ifdef VERBOSE_STATISTICS
//				tdata->restructures_at_level[level]++;
//#				endif
//			} else if (IS_BLACK(q->link[!dir])) {
//				rbt_node_t *s = p->link[!last];
//
//				if (s) {
//					if (IS_BLACK(s->link[!last]) && IS_BLACK(s->link[last])) {
//						p->is_red = 0;
//						q->is_red = 1;
//						s->is_red = 1;
//#						ifdef VERBOSE_STATISTICS
//						tdata->restructures_at_level[level]++;
//#						endif
//					} else {
//						int dir2 = (g->link[1] == p);
//
//						if (IS_RED(s->link[last])) {
//							g->link[dir2] = rbt_rotate_double(p, last);
//							p->is_red = 0;
//							q->is_red = 1;
//#							ifdef VERBOSE_STATISTICS
//							tdata->restructures_at_level[level]++;
//#							endif
//						} else if (IS_RED(s->link[!last])) {
//							g->link[dir2] = rbt_rotate_single(p, last);
//							p->is_red = 0;
//							q->is_red = 1;
//							s->is_red = 1;
//							s->link[!last]->is_red = 0;
//#							ifdef VERBOSE_STATISTICS
//							tdata->restructures_at_level[level]++;
//#							endif
//						}
//					}
//				}
//			}
//		}
//	}
//
//	/* q is external. */
//	if (q->key == key) {
//		ret = 1;
//		*node_to_delete = q;
//		if (!p) {
//			rbt->root = NULL;
//		} else {
//			if (!g)
//				g = rbt->root;
//
//			int last = g->key < key;
//			dir = p->key < key;
//			g->link[last] = p->link[!dir];
//		}
//	}
//
//	/* Update root and make it BLACK. */
//	if (rbt->root != head.link[1])
//		rbt->root = head.link[1];
//	if (rbt->root && rbt->root->is_red == 1)
//		rbt->root->is_red = 0;
//	
//	return ret;
//}
//
//static int bh;
//static int paths_with_bh_diff;
//static int total_paths;
//static int min_path_len, max_path_len;
//static int total_nodes, red_nodes, black_nodes;
//static int red_red_violations, bst_violations;
//static void _rbt_validate_rec(rbt_node_t *root, int _bh, int _th)
//{
//	if (!root)
//		return;
//
//	rbt_node_t *left = root->link[0];
//	rbt_node_t *right = root->link[1];
//
//	total_nodes++;
//	black_nodes += (IS_BLACK(root));
//	red_nodes += (IS_RED(root));
//	_th++;
//	_bh += (IS_BLACK(root));
//
//	/* BST violation? */
//	if (left && left->key > root->key)
//		bst_violations++;
//	if (right && right->key <= root->key)
//		bst_violations++;
//
//	/* Red-Red violation? */
//	if (IS_RED(root) && (IS_RED(left) || IS_RED(right)))
//		red_red_violations++;
//
//	/* We found a path (a node with at least one sentinel child). */
//	if (!left || !right) {
//		total_paths++;
//		if (bh == -1)
//			bh = _bh;
//		else if (_bh != bh)
//			paths_with_bh_diff++;
//
//		if (_th <= min_path_len)
//			min_path_len = _th;
//		if (_th >= max_path_len)
//			max_path_len = _th;
//	}
//
//	/* Check subtrees. */
//	if (left)
//		_rbt_validate_rec(left, _bh, _th);
//	if (right)
//		_rbt_validate_rec(right, _bh, _th);
//}
//
//static inline int _rbt_validate_helper(rbt_node_t *root)
//{
//	int check_bh = 0, check_red_red = 0, check_bst = 0;
//	int check_rbt = 0;
//	bh = -1;
//	paths_with_bh_diff = 0;
//	total_paths = 0;
//	min_path_len = 99999999;
//	max_path_len = -1;
//	total_nodes = black_nodes = red_nodes = 0;
//	red_red_violations = 0;
//	bst_violations = 0;
//
//	_rbt_validate_rec(root, 0, 0);
//
//	check_bh = (paths_with_bh_diff == 0);
//	check_red_red = (red_red_violations == 0);
//	check_bst = (bst_violations == 0);
//	check_rbt = (check_bh && check_red_red && check_bst);
//
//	printf("Validation:\n");
//	printf("=======================\n");
//	printf("  Valid Red-Black Tree: %s\n",
//	       check_rbt ? "Yes [OK]" : "No [ERROR]");
//	printf("  Black height: %d [%s]\n", bh,
//	       check_bh ? "OK" : "ERROR");
//	printf("  Red-Red Violation: %s\n",
//	       check_red_red ? "No [OK]" : "Yes [ERROR]");
//	printf("  BST Violation: %s\n",
//	       check_bst ? "No [OK]" : "Yes [ERROR]");
//	printf("  Tree size (Total / Black / Red): %8d / %8d / %8d\n",
//	       total_nodes, black_nodes, red_nodes);
//	printf("  Total paths: %d\n", total_paths);
//	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
//	printf("\n");
//
//	return check_rbt;
//}
//
//static rbt_node_t *_rbt_warmup_insert_rec(rbt_t *rbt, rbt_node_t *root, 
//                                   rbt_node_t *node[2], int *found, int level)
//{
//	if (!root) {
//		root = node[0];
//		return root;
//	}
//
//	if (IS_EXTERNAL_NODE(root) && root->key == node[0]->key) {
//		*found = 1;
//		return root;
//	}
//
//	if (IS_EXTERNAL_NODE(root)) {
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
//
//		return root;
//	}
//
//	int dir = root->key < node[0]->key;
//	rbt_node_t *new_link_dir = _rbt_warmup_insert_rec(rbt, root->link[dir],
//	                                           node, found, level + 1);
//
//	if (root->link[dir] != new_link_dir)
//		root->link[dir] = new_link_dir;
//
//	/* DEBUG */
//	if (*found)
//		return root;
//
//	/* If we caused a Red-Red violation let's fix it. */
//	if (IS_BLACK(root->link[dir]))
//		return root;
//
//	if (IS_BLACK(root->link[dir]->link[0]) && 
//	    IS_BLACK(root->link[dir]->link[1]))
//		return root;
//
//	/* root->link[dir] is red with one red child. */
//	if (IS_RED(root->link[!dir])) {
//		/* Case 1 */
//		root->is_red = 1;
//		root->link[dir]->is_red = 0;
//		root->link[!dir]->is_red = 0;
//	} else if (IS_BLACK(root->link[!dir])) {
//		if (IS_RED(root->link[dir]->link[dir])) {
//			/* Case 2 */
//			root->is_red = 1;
//			root->link[dir]->is_red = 0;
//			root = rbt_rotate_single(root, !dir);
//		} else {
//			/* Case 3 */
//			root->is_red = 1;
//			root->link[dir]->link[!dir]->is_red = 0;
//			root = rbt_rotate_double(root, !dir);
//		}
//	}
//
//	return root;
//}
//
//static inline int _rbt_warmup_insert_helper(rbt_t *rbt, rbt_node_t *nodes[2])
//{
//	int found = 0;
//
//	rbt_node_t *new_root = _rbt_warmup_insert_rec(rbt, rbt->root, nodes,
//	                                       &found, 0);
//	if (rbt->root != new_root)
//		rbt->root = new_root;
//	if (IS_RED(rbt->root))
//		rbt->root->is_red = 0;
//
//	return !found;
//}
//
//static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
//                                     unsigned int seed, int force)
//{
//	int i, nodes_inserted = 0, ret = 0;
//	rbt_node_t *nodes[2];
//	
//	srand(seed);
//	while (nodes_inserted < nr_nodes) {
//		int key = rand() % max_key;
//		nodes[0] = rbt_node_new(key, NULL);
//		nodes[1] = rbt_node_new(key, NULL);
//
//		ret = _rbt_warmup_insert_helper(rbt, nodes);
//		nodes_inserted += ret;
//
//		if (!ret) {
//			free(nodes[0]);
//			free(nodes[1]);
//		}
//	}
//
//	return nodes_inserted;
//}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
//	printf("Size of tree node is %lu\n", sizeof(rbt_node_t));
//	return _rbt_new_helper();
	return NULL;
}

void *rbt_thread_data_new(int tid)
{
//	td_ext_thread_data_t *data = td_ext_thread_data_new(tid);
//
//	return data;
	return NULL;
}

void rbt_thread_data_print(void *thread_data)
{
//	td_ext_thread_data_t *tdata = thread_data;
//
//	td_ext_thread_data_print(tdata);
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
//	td_ext_thread_data_t *_d1 = d1, *_d2 = d2, *_dst = dst;
//
//	td_ext_thread_data_add(_d1, _d2, _dst);
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret = 0;
//	td_ext_thread_data_t *tdata = thread_data;
//
//	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
//#	endif
//
//	ret = _rbt_lookup_helper(rbt, key);
//
//#	if defined(SYNC_CG_SPINLOCK)
//	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
//#	elif defined(SYNC_CG_HTM)
//	int tx_ret = tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
//#	if defined(STATS_LACQS_PER_LEVEL)
//	lacqs_per_level[ret] += tx_ret;
//#	endif
//#	endif

	return (ret != 0);
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
//	rbt_node_t *nodes[2];
//	td_ext_thread_data_t *tdata = thread_data;
//
//	nodes[0] = rbt_node_new(key, value);
//	nodes[1] = rbt_node_new(key, value);
//
//#	if defined(SYNC_CG_SPINLOCK)
//	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
//#	elif defined(SYNC_CG_HTM)
//	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
//#	endif
//
//	ret = _rbt_insert_helper(rbt, nodes, thread_data);
//
//#	if defined(SYNC_CG_SPINLOCK)
//	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
//#	elif defined(SYNC_CG_HTM)
//	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
//#	endif
//
//	if (!ret) {
//		free(nodes[0]);
//		free(nodes[1]);
//	}

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;
//	rbt_node_t *node_to_delete = NULL;
//	td_ext_thread_data_t *tdata = thread_data;
//
//#	if defined(SYNC_CG_SPINLOCK)
//	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
//#	elif defined(SYNC_CG_HTM)
//	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
//#	endif
//
//	ret = _rbt_delete_helper(rbt, key, &node_to_delete, thread_data);
//
//#	if defined(SYNC_CG_SPINLOCK)
//	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
//#	elif defined(SYNC_CG_HTM)
//	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
//#	endif
//
//	free(node_to_delete);

	return ret;
}

int rbt_validate(void *rbt)
{
//	int ret;
//	ret = _rbt_validate_helper(((rbt_t *)rbt)->root);
//	return ret;
	return 1;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
//	int ret;
//	ret = _rbt_warmup_helper((rbt_t *)rbt, nr_nodes, max_key, seed, force);
//	return ret;
	return 0;
}

char *rbt_name()
{
	return "rbt_natarajan";
}
