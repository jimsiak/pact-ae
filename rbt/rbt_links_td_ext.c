#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm.h"
#	if !defined(TX_NUM_RETRIES)
#		define TX_NUM_RETRIES 20
#	endif
#endif

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
//} rbt_node_t;

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

static inline int _rbt_insert_helper(rbt_t *rbt, rbt_node_t *node[2],
                                     td_ext_thread_data_t *tdata)
{
	if (!rbt->root) {
		rbt->root = node[0];
		rbt->root->is_red = 0;
		return 1;
	}

	/* gg = grandgrandparent, g = grandparent, p = parent, q = current. */
	rbt_node_t *gg, *g, *p, *q;
	rbt_node_t head = {0}; /* False tree root. */
	int dir = 0, last;
	int inserted = 0;
#	ifdef VERBOSE_STATISTICS
	int level = 0;
#	endif

	gg = &head;
	g = p = NULL;
	q = gg->link[1] = rbt->root;

	/* Search down the tree */
	while (1) {
		if (IS_EXTERNAL_NODE(q)) {
			if (q->key == node[0]->key)
				break;

			/* Insert new node at the bottom */
			q->link[0] = node[0];
			q->link[1] = node[1];
			q->is_red = 1;
			q->link[0]->is_red = 0;
			q->link[1]->is_red = 0;

			if (q->key > node[0]->key) {
				q->link[1]->key = q->key;
				q->key = q->link[0]->key;
			} else {
				q->link[0]->key = q->key;
			}
			inserted = 1;
		} else if (IS_RED(q->link[0]) && IS_RED(q->link[1])) {
			/* Color flip */
			q->is_red = 1;
			q->link[0]->is_red = 0;
			q->link[1]->is_red = 0;

#			ifdef VERBOSE_STATISTICS
			tdata->restructures_at_level[level]++;
#			endif
		}
		
		/* Fix red violation */
		if (IS_RED(q) && IS_RED(p)) {
			int dir2 = (gg->link[1] == g);
			
			g->is_red = 1;
			if (q == p->link[last]) {
				p->is_red = 0;
				gg->link[dir2] = rbt_rotate_single(g, !last);

				last = dir;
				dir = q->key < node[0]->key;
				g = p;
				p = q;
				q = p->link[dir];

#				ifdef VERBOSE_STATISTICS
				tdata->restructures_at_level[level]++;
#				endif

				continue;
			} else {
				q->is_red = 0;
				gg->link[dir2] = rbt_rotate_double(g, !last);

				last = q->key < node[0]->key;
				dir = q->link[last]->key < node[0]->key;
				g = q;
				p = g->link[last];
				q = p->link[dir];

#				ifdef VERBOSE_STATISTICS
				tdata->restructures_at_level[level]++;
#				endif

				continue;
			}
		}

		last = dir;
		dir = q->key < node[0]->key;
		
		/* Update helpers */
		if (g)
			gg = g;
		g = p;
		p = q;
		q = q->link[dir];

#		ifdef VERBOSE_STATISTICS
		tdata->passed_from_level[level]++;
		level++;
#		endif
	}

	/* Update root and make it BLACK */
	if (rbt->root != head.link[1])
		rbt->root = head.link[1];
	if (IS_RED(rbt->root))
		rbt->root->is_red = 0;

	return inserted;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, rbt_node_t **node_to_delete,
                              td_ext_thread_data_t *tdata)
{
	if (!rbt->root)
		return 0;

	/* g = grandparent, p = parent, q = current */
	rbt_node_t *g, *p, *q;
	rbt_node_t *f = NULL;
	rbt_node_t head = { 0 };
	int dir = 1;
	int ret = 0;
#	ifdef VERBOSE_STATISTICS
	int level = -1;
#	endif

	/* Set up helpers */
	q = &head;
	g = p = NULL;
	q->link[1] = rbt->root;

	while (!IS_EXTERNAL_NODE(q)) {
		int last = dir;

		/* Update helpers. */
		g = p;
		p = q;
		q = q->link[dir];
		dir = q->key < key;
#		ifdef VERBOSE_STATISTICS
		level++;
		tdata->passed_from_level[level]++;
#		endif

		if (IS_EXTERNAL_NODE(q))
			break;

		if (IS_BLACK(q) && IS_BLACK(q->link[dir])) {
			if (IS_RED(q->link[!dir])) {
				q->is_red = 1;
				q->link[!dir]->is_red = 0;
				p = p->link[last] = rbt_rotate_single(q, dir);
#				ifdef VERBOSE_STATISTICS
				tdata->restructures_at_level[level]++;
#				endif
			} else if (IS_BLACK(q->link[!dir])) {
				rbt_node_t *s = p->link[!last];

				if (s) {
					if (IS_BLACK(s->link[!last]) && IS_BLACK(s->link[last])) {
						p->is_red = 0;
						q->is_red = 1;
						s->is_red = 1;
#						ifdef VERBOSE_STATISTICS
						tdata->restructures_at_level[level]++;
#						endif
					} else {
						int dir2 = (g->link[1] == p);

						if (IS_RED(s->link[last])) {
							g->link[dir2] = rbt_rotate_double(p, last);
							p->is_red = 0;
							q->is_red = 1;
#							ifdef VERBOSE_STATISTICS
							tdata->restructures_at_level[level]++;
#							endif
						} else if (IS_RED(s->link[!last])) {
							g->link[dir2] = rbt_rotate_single(p, last);
							p->is_red = 0;
							q->is_red = 1;
							s->is_red = 1;
							s->link[!last]->is_red = 0;
#							ifdef VERBOSE_STATISTICS
							tdata->restructures_at_level[level]++;
#							endif
						}
					}
				}
			}
		}
	}

	/* q is external. */
	if (q->key == key) {
		ret = 1;
		*node_to_delete = q;
		if (!p) {
			rbt->root = NULL;
		} else {
			if (!g)
				g = rbt->root;

			int last = g->key < key;
			dir = p->key < key;
			g->link[last] = p->link[!dir];
		}
	}

	/* Update root and make it BLACK. */
	if (rbt->root != head.link[1])
		rbt->root = head.link[1];
	if (rbt->root && rbt->root->is_red == 1)
		rbt->root->is_red = 0;
	
	return ret;
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

//static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
//                                     unsigned int seed, int force)
//{
//	int i, nodes_inserted = 0, ret = 0;
//	rbt_node_t *nodes[2];
//	td_ext_thread_data_t *tdata = td_ext_thread_data_new(-1);
//	
//	srand(seed);
//	while (nodes_inserted < nr_nodes) {
//		int key = rand() % max_key;
//		nodes[0] = rbt_node_new(key, NULL);
//		nodes[1] = rbt_node_new(key, NULL);
//		ret = _rbt_insert_helper(rbt, nodes, tdata);
//		nodes_inserted += ret;
//
//		if (!ret) {
//			free(nodes[0]);
//			free(nodes[1]);
//		}
//	}
//
//	free(tdata);
//	return nodes_inserted;
//}

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
	rbt_node_t *node_to_delete = NULL;
	td_ext_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

	ret = _rbt_delete_helper(rbt, key, &node_to_delete, thread_data);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((rbt_t *)rbt)->rbt_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((rbt_t *)rbt)->rbt_lock);
#	endif

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
	return "links_td_external";
}
