#include <pthread.h> //> pthread_spinlock_t

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )
#define IS_BLACK(node) ( !(node) || !(node)->is_red )
#define IS_RED(node) ( !IS_BLACK(node) )

typedef struct rbt_node {
	int is_red;
	int key;
	void *value;
	struct rbt_node *link[2];

	pthread_spinlock_t spinlock; /* per node lock */

	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) - 
	             2 * sizeof(struct rbt_node *) - sizeof(pthread_spinlock_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;
//} rbt_node_t;

typedef struct {
	rbt_node_t *root;

	pthread_spinlock_t spinlock;
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

	pthread_spin_init(&ret->spinlock, PTHREAD_PROCESS_SHARED);

	return ret;
}

static inline rbt_t *_rbt_new_helper()
{
	rbt_t *ret;

	XMALLOC(ret, 1);
	ret->root = NULL;

	pthread_spin_init(&ret->spinlock, PTHREAD_PROCESS_SHARED);

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
	int dir, found = 0;
	rbt_node_t *curr, *curr_saved;

	pthread_spin_lock(&rbt->spinlock);

	/* Empty tree? */
	if (!rbt->root) {
		pthread_spin_unlock(&rbt->spinlock);
		return 0;
	}

	curr = rbt->root;
	pthread_spin_lock(&rbt->root->spinlock);
	pthread_spin_unlock(&rbt->spinlock);

	while (1) {
		/* External node reached. */
		if (IS_EXTERNAL_NODE(curr)) {
			found = (curr->key == key);
			break;
		}

		curr_saved = curr;
		dir = curr->key < key;
		curr = curr->link[dir];

		pthread_spin_lock(&curr->spinlock);
		pthread_spin_unlock(&curr_saved->spinlock);
	}

	pthread_spin_unlock(&curr->spinlock);
	return found;
}

static int _rbt_insert_helper_fg(rbt_t *rbt, rbt_node_t *node[2])
{
	pthread_spin_lock(&rbt->spinlock);
	if (!rbt->root) {
		rbt->root = node[0];
		rbt->root->is_red = 0;
		pthread_spin_unlock(&rbt->spinlock);
		return 1;
	}

	/* gg = grandgrandparent, g = grandparent, p = parent, q = current. */
	rbt_node_t *gg, *g, *p, *q;
	rbt_node_t head = {0}; /* False tree root. */
	int dir = 0, last = 0;
	int inserted = 0;

	pthread_spin_init(&head.spinlock, PTHREAD_PROCESS_SHARED);

	gg = &head;
	g = p = NULL;
	q = gg->link[1] = rbt->root;

	pthread_spin_lock(&q->spinlock);

	/* Search down the tree */
	while (1) {
		if (q->link[0]) pthread_spin_lock(&q->link[0]->spinlock);
		if (q->link[1]) pthread_spin_lock(&q->link[1]->spinlock);

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

			pthread_spin_lock(&q->link[0]->spinlock);
			pthread_spin_lock(&q->link[1]->spinlock);
		} else if (IS_RED(q->link[0]) && IS_RED(q->link[1])) {
			/* Color flip */
			q->is_red = 1;
			q->link[0]->is_red = 0;
			q->link[1]->is_red = 0;
		}
		
		/* Fix red violation */
		if (IS_RED(q) && IS_RED(p)) {
			if (!g) printf("Errorrrrrrr key %10d\n", node[0]->key);

			int dir2 = (gg->link[1] == g);
			
			g->is_red = 1;
			if (q == p->link[last]) {
				p->is_red = 0;
				gg->link[dir2] = rbt_rotate_single(g, !last);

				if (gg == &head)
					rbt->root = gg->link[dir2];

				last = dir;
				dir = q->key < node[0]->key;

				/* Release locks before moving down. */
				if (g) pthread_spin_unlock(&g->spinlock);
				if (q->link[!dir]) pthread_spin_unlock(&q->link[!dir]->spinlock);

				g = p;
				p = q;
				q = p->link[dir];

				continue;
			} else {
				q->is_red = 0;
				gg->link[dir2] = rbt_rotate_double(g, !last);

				if (gg == &head)
					rbt->root = gg->link[dir2];

				last = q->key < node[0]->key;
				dir = q->link[last]->key < node[0]->key;

				/* Release locks before moving down. */
				if (q->link[!last]) 
					pthread_spin_unlock(&q->link[!last]->spinlock);
				if (q->link[!last]->link[!dir])
					pthread_spin_unlock(&q->link[!last]->link[!dir]->spinlock);

				g = q;
				p = g->link[last];
				q = p->link[dir];

				continue;
			}
		}

		last = dir;
		dir = q->key < node[0]->key;
		
		if (gg == rbt->root) {
			if (IS_RED(rbt->root))
				rbt->root->is_red = 0;
			pthread_spin_unlock(&rbt->spinlock);
		}

		/* Release locks before moving down. */
		if (q->link[!dir])
			pthread_spin_unlock(&q->link[!dir]->spinlock);
		if (g && gg)
			pthread_spin_unlock(&gg->spinlock);

		/* Update helpers */
		if (g)
			gg = g;
		g = p;
		p = q;
		q = q->link[dir];

	}

	/* Update root and make it BLACK */
	if (gg == rbt->root || gg == &head) {
		if (rbt->root != head.link[1])
			rbt->root = head.link[1];
		if (IS_RED(rbt->root))
			rbt->root->is_red = 0;
		pthread_spin_unlock(&rbt->spinlock);
	}

	/* Release all locks. */
	if (gg) pthread_spin_unlock(&gg->spinlock);
	if (g) pthread_spin_unlock(&g->spinlock);
	if (p) pthread_spin_unlock(&p->spinlock);
	if (q) pthread_spin_unlock(&q->spinlock);

	return inserted;
}

static inline int _rbt_insert_helper_serial(rbt_t *rbt, rbt_node_t *node[2])
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
				continue;
			} else {
				q->is_red = 0;
				gg->link[dir2] = rbt_rotate_double(g, !last);

				last = q->key < node[0]->key;
				dir = q->link[last]->key < node[0]->key;
				g = q;
				p = g->link[last];
				q = p->link[dir];
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
	}

	/* Update root and make it BLACK */
	if (rbt->root != head.link[1])
		rbt->root = head.link[1];
	if (IS_RED(rbt->root))
		rbt->root->is_red = 0;

	return inserted;
}

static inline int _rbt_delete_helper_fg(rbt_t *rbt, int key, 
                                     rbt_node_t *nodes_to_delete[2])
{
	/* Empty tree. */
	pthread_spin_lock(&rbt->spinlock);
	if (!rbt->root) {
		pthread_spin_unlock(&rbt->spinlock);
		return 0;
	}

	/* Tree with one node only. */
	pthread_spin_lock(&rbt->root->spinlock);
	if (IS_EXTERNAL_NODE(rbt->root)) {
		if (rbt->root->key == key) {
			nodes_to_delete[0] = rbt->root;
			nodes_to_delete[1] = NULL;
			rbt->root = NULL;
			pthread_spin_unlock(&rbt->spinlock);
			return 1;
		}
		pthread_spin_unlock(&rbt->root->spinlock);
		pthread_spin_unlock(&rbt->spinlock);
		return 0;
	}
	pthread_spin_unlock(&rbt->root->spinlock);

	/* g = grandparent, p = parent, q = current */
	rbt_node_t *g, *p, *q;
	rbt_node_t *s = NULL; /* sibling */
	rbt_node_t head = { 0 };
	int dir = 1, last;
	int ret = 0;
	int released_global_lock = 0;

	pthread_spin_init(&head.spinlock, PTHREAD_PROCESS_SHARED);

	/* Set up helpers */
	p = g = NULL;
	q = &head;
	q->link[1] = rbt->root;

	pthread_spin_lock(&q->link[1]->spinlock);

	while (!IS_EXTERNAL_NODE(q->link[dir])) {
		last = dir;

		if ((g == &head || g == rbt->root) && !released_global_lock) {
			pthread_spin_unlock(&rbt->spinlock);
			released_global_lock = 1;
		}

		/* Here we own g, p, q, q->link[0], q->link[1] */
		rbt_node_t *g_saved = g, *q_saved = q;
		g = p;
		p = q;
		q = q->link[dir];
		dir = q->key < key;

		/* Grab the next locks and release the previous. */
		pthread_spin_lock(&q->link[0]->spinlock);
		pthread_spin_lock(&q->link[1]->spinlock);
		if (g_saved) pthread_spin_unlock(&g_saved->spinlock);
		if (q_saved->link[!last])
			pthread_spin_unlock(&q_saved->link[!last]->spinlock);

		if (IS_BLACK(q) && IS_BLACK(q->link[dir])) {
			if (IS_RED(q->link[!dir])) {
				rbt_node_t *p_saved = p;
				q->is_red = 1;
				q->link[!dir]->is_red = 0;
				p = p->link[last] = rbt_rotate_single(q, dir);

				if (q == rbt->root)
					rbt->root = p;
					
				pthread_spin_unlock(&p_saved->spinlock);
				pthread_spin_lock(&q->link[!dir]->spinlock);
			} else if (IS_BLACK(q->link[!dir])) {
				s = p->link[!last];

				if (s) {
					pthread_spin_lock(&s->spinlock);
					pthread_spin_lock(&s->link[0]->spinlock);
					pthread_spin_lock(&s->link[1]->spinlock);
					rbt_node_t *s_saved = s;
					rbt_node_t *s_link_saved[2] = { s->link[0], s->link[1] };

					if (IS_BLACK(s->link[!last]) && IS_BLACK(s->link[last])) {
						p->is_red = 0;
						q->is_red = 1;
						s->is_red = 1;
					} else {
						int dir2 = (g->link[1] == p);

						if (IS_RED(s->link[last])) {
							g->link[dir2] = rbt_rotate_double(p, last);
							p->is_red = 0;
							q->is_red = 1;

							if (p == rbt->root)
								rbt->root = head.link[1];

						} else if (IS_RED(s->link[!last])) {
							g->link[dir2] = rbt_rotate_single(p, last);
							p->is_red = 0;
							q->is_red = 1;
							s->is_red = 1;
							s->link[!last]->is_red = 0;

							if (p == rbt->root)
								rbt->root = head.link[1];

						}
					}

					pthread_spin_unlock(&s_saved->spinlock);
					pthread_spin_unlock(&s_link_saved[0]->spinlock);
					pthread_spin_unlock(&s_link_saved[1]->spinlock);
				}
			}
		}
	}

	/* q->link[dir] is external. */
	if (q->link[dir]->key == key) {
		ret = 1;
		nodes_to_delete[0] = q;
		nodes_to_delete[1] = q->link[dir];

		last = p->key < key;
		dir = q->key < key;
		p->link[last] = q->link[!dir];

		if (p == &head)
			rbt->root = p->link[last];
	}

	/* Unlock all locked nodes */
	if (g) pthread_spin_unlock(&g->spinlock);
	if (p) pthread_spin_unlock(&p->spinlock);
	if (q) pthread_spin_unlock(&q->spinlock);
	if (q->link[0]) pthread_spin_unlock(&q->link[0]->spinlock);
	if (q->link[1]) pthread_spin_unlock(&q->link[1]->spinlock);

	if ((!p || p == &head || p == rbt->root || g == &head || g == rbt->root) && 
	    !released_global_lock) {
		pthread_spin_unlock(&rbt->spinlock);
		released_global_lock = 1;
	}

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

		ret = _rbt_insert_helper_serial(rbt, nodes);
		nodes_inserted += ret;

		if (!ret) {
			free(nodes[0]);
			free(nodes[1]);
		}
	}

	return nodes_inserted;
}

/**
 * Returns the number of locks in the tree that are locked.
 **/
static unsigned int rbt_check_locks(rbt_node_t *root)
{
	if (!root)
		return 0;

	int myret = root->spinlock; /* myret = 1 if locked, 0 otherwise. */
	return myret + rbt_check_locks(root->link[0]) + 
	               rbt_check_locks(root->link[1]);
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
	int ret;

	ret = _rbt_lookup_helper(rbt, key);

	return ret;
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret;
	rbt_node_t *nodes[2];

	nodes[0] = rbt_node_new(key, value);
	nodes[1] = rbt_node_new(key, value);

	ret = _rbt_insert_helper_fg(rbt, nodes);

	if (!ret) {
		free(nodes[0]);
		free(nodes[1]);
	}

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret;
	rbt_node_t *nodes_to_delete[2];

	ret = _rbt_delete_helper_fg(rbt, key, nodes_to_delete);

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
	return "links_td_external_fg_spinlock";
}
