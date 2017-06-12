#include <pthread.h> //> pthread_spinlock_t
#include <htmintrin.h> /* power8 tm gcc intrinsics. */
#include <assert.h>

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h" /* XMALLOC() */

#define TX_STATS_ARRAY_NR_TRANS 2
#include "rbt_links_td_ext_fg_htm_thread_data.h"

#if !defined(TX_NUM_RETRIES)
#	define TX_NUM_RETRIES 20
#endif

#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )
#define IS_BLACK(node) ( !(node) || !(node)->is_red )
#define IS_RED(node) ( !IS_BLACK(node) )
#define GET_VERSION(node) ( (!(node)) ? 0 : (node)->version )
//#define INC_VERSION(node) (node)->version++
#define INC_VERSION(node) do { if ((node))  { (node)->version++; } } while(0)

#define IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN 0xfe
#define IMPLICIT_ABORT_VERSION_ERROR 0xee

typedef struct rbt_node {
	int is_red;
	int key;
	void *value;
	struct rbt_node *link[2];

	unsigned long long int version; /* A version numbers, starts from 1. */

	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) - sizeof(void *) - 
	             2 * sizeof(struct rbt_node *) - sizeof(unsigned long long)];
} __attribute__((aligned(CACHE_LINE_SIZE))) rbt_node_t;

typedef struct {
	rbt_node_t *root;

	unsigned long long int version;
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
	ret->version = 1;

	return ret;
}

static inline rbt_t *_rbt_new_helper()
{
	rbt_t *ret;

	XMALLOC(ret, 1);
	ret->root = NULL;
	ret->version = 1;

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

static int _rbt_lookup_helper_serial(rbt_t *rbt, int key)
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

static int _rbt_lookup_helper(rbt_t *rbt, void *thread_data, int key,
                              htm_fg_tdata_t *tdata)
{
	int dir, found;
	rbt_node_t *curr;
	unsigned long long window_versions[1]; /* curr version */
	int retries = -1, window_retries = -1;

try_from_scratch:

	retries++;
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_lookup_helper_serial(rbt, key);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	/* Avoid Lemming effect. */
	while (rbt->spinlock == 1)
		;

	/* First transaction at the root. */
	tdata->tx_starts++;
	tdata->tx_stats[0][0][0]++;
	if (__builtin_tbegin(0)) {
		if (rbt->spinlock == 1)
			__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

		/* Empty tree. */
		if (!rbt->root) {
			__builtin_tend(0);
			return 0;
		}

		curr = rbt->root;
		window_versions[0] = GET_VERSION(curr);
		__builtin_tend(0);
	} else {
		tdata->tx_aborts++;
		tdata->tx_stats[0][0][1]++;
		texasru_t texasru = __builtin_get_texasru();
		if (_TEXASRU_ABORT(texasru) && 
		    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
			tdata->tx_aborts_version_error++;
			tdata->tx_stats[0][0][5]++;
		} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
			tdata->tx_stats[0][0][4]++;
			tdata->tx_aborts_footprint_overflow++;
		} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
			tdata->tx_stats[0][0][2]++;
			tdata->tx_aborts_transaction_conflict++;
		} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
			tdata->tx_stats[0][0][3]++;
			tdata->tx_aborts_non_transaction_conflict++;
		} else {
			tdata->tx_stats[0][0][6]++;
			tdata->tx_aborts_rest++;
		}

		goto try_from_scratch;
	}

	/* Walk down the tree. */
	while (1) {
		window_retries = -1;

retry_window:

		window_retries++;
		if (window_retries >= TX_NUM_RETRIES)
			goto try_from_scratch;

		/* Avoid Lemming effect. */
		while (rbt->spinlock == 1)
			;

		tdata->tx_starts++;
		tdata->tx_stats[0][1][0]++;
		if (__builtin_tbegin(0)) {
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			if (window_versions[0] != GET_VERSION(curr))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			/* External node reached. */
			if (IS_EXTERNAL_NODE(curr)) {
				found = (curr->key == key);
				__builtin_tend(0);
				return found;
			}

			/* Move to the next node. */
			dir = curr->key < key;
			curr = curr->link[dir];

			window_versions[0] = GET_VERSION(curr);

			__builtin_tend(0);
		} else {
			tdata->tx_aborts++;
			tdata->tx_stats[0][1][1]++;
			texasru_t texasru = __builtin_get_texasru();
			if (_TEXASRU_ABORT(texasru) && 
			    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
				tdata->tx_aborts_version_error++;
				tdata->tx_stats[0][1][5]++;
				goto try_from_scratch;
			} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
				tdata->tx_stats[0][1][4]++;
				tdata->tx_aborts_footprint_overflow++;
			} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
				tdata->tx_stats[0][1][2]++;
				tdata->tx_aborts_transaction_conflict++;
			} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
				tdata->tx_stats[0][1][3]++;
				tdata->tx_aborts_non_transaction_conflict++;
			} else {
				tdata->tx_stats[0][1][6]++;
				tdata->tx_aborts_rest++;
			}
			goto retry_window;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
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

/**
 * Called from `_rbt_insert_helper_fg`, when the global lock
 * is acquired, so there is no need for synchronization.
 * We only need to modify the nodes versions to notify transactions
 * which will continue running afterwards.
 */
static int _rbt_insert_helper_fg_serial(rbt_t *rbt, rbt_node_t *node[2])
{
	if (!rbt->root) {
		rbt->root = node[0];
		rbt->root->is_red = 0;
		rbt->version++;
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

			INC_VERSION(q);
			INC_VERSION(q->link[0]);
			INC_VERSION(q->link[1]);
			if (q == rbt->root) {
				q->is_red = 0;
				rbt->version++;
			} else {
				INC_VERSION(p);
			}
		} else if (IS_RED(q->link[0]) && IS_RED(q->link[1])) {
			/* Color flip */
			q->is_red = 1;
			q->link[0]->is_red = 0;
			q->link[1]->is_red = 0;

			INC_VERSION(q);
			INC_VERSION(q->link[0]);
			INC_VERSION(q->link[1]);
			if (q == rbt->root) {
				q->is_red = 0;
				rbt->version++;
			} else {
				INC_VERSION(p);
			}
		}
		
		/* Fix red violation */
		if (IS_RED(q) && IS_RED(p)) {
			int dir2 = (gg->link[1] == g);
			
			g->is_red = 1;
			if (q == p->link[last]) {
				p->is_red = 0;
				gg->link[dir2] = rbt_rotate_single(g, !last);

				if (gg == &head)
					rbt->version++;
				INC_VERSION(gg);
				INC_VERSION(g);
				INC_VERSION(p);
				INC_VERSION(q);

				last = dir;
				dir = q->key < node[0]->key;
				g = p;
				p = q;
				q = p->link[dir];

				continue;
			} else {
				q->is_red = 0;
				gg->link[dir2] = rbt_rotate_double(g, !last);

				if (gg == &head)
					rbt->version++;
				INC_VERSION(gg);
				INC_VERSION(g);
				INC_VERSION(p);
				INC_VERSION(q);

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
	if (rbt->root != head.link[1]) {
		rbt->root = head.link[1];
		rbt->version++;
	}
	if (IS_RED(rbt->root)) {
		rbt->root->is_red = 0;
		rbt->version++;
	}

	return inserted;
}

static int _rbt_insert_helper_fg(rbt_t *rbt, rbt_node_t *node[2],
                                 htm_fg_tdata_t *tdata)
{
	/* gg = grandgrandparent, g = grandparent, p = parent, q = current. */
	rbt_node_t *gg, *g, *p, *q;
	rbt_node_t head = {0}; /* False tree root. */
	int i, dir = 0, last = 0, inserted = 0, retries = -1, window_retries = -1;
	unsigned int level;
	unsigned long long window_versions[5]; /* gg, g, p, q, rbt versions. */

try_from_scratch:

	retries++;
	/* Grab the global lock and serialize. */
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_insert_helper_fg_serial(rbt, node);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	head.is_red = 0;
	head.version = 1;

	level = 0;
	dir = 0;

	/* First transaction at the root. */
	while (1) {
		while (rbt->spinlock == 1)
			;

		tdata->tx_stats[1][0][0]++;
		tdata->tx_starts++;
		if (__builtin_tbegin(0)) {
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

			/* Empty tree. */
			if (!rbt->root) {
				rbt->root = node[0];
				rbt->root->is_red = 0;

				INC_VERSION(rbt->root);
				rbt->version++;
				__builtin_tend(0);
				return 1;
			}

			dir = 0;

			gg = &head;
			g = p = NULL;
			q = gg->link[1] = rbt->root;

			window_versions[0] = GET_VERSION(gg);
			window_versions[1] = GET_VERSION(g);
			window_versions[2] = GET_VERSION(p);
			window_versions[3] = GET_VERSION(q);
			window_versions[4] = rbt->version;
			__builtin_tend(0);
			break;
		} else {
			/* Abort. */
			tdata->tx_aborts++;
			tdata->tx_stats[1][0][1]++;
			texasru_t texasru = __builtin_get_texasru();
			if (_TEXASRU_ABORT(texasru) && 
			    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
				tdata->tx_stats[1][0][5]++;
				tdata->tx_aborts_version_error++;
			} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
				tdata->tx_stats[1][0][4]++;
				tdata->tx_aborts_footprint_overflow++;
			} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
				tdata->tx_stats[1][0][2]++;
				tdata->tx_aborts_transaction_conflict++;
			} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
				tdata->tx_stats[1][0][3]++;
				tdata->tx_aborts_non_transaction_conflict++;
			} else {
				tdata->tx_stats[1][0][6]++;
				tdata->tx_aborts_rest++;
			}

			goto try_from_scratch;
		}
	}

	/* Search down the tree */
	while (1) {
		window_retries = -1;
retry_window:

		if (inserted)
			return 1;

		window_retries++;
		if (window_retries >= TX_NUM_RETRIES)
			goto try_from_scratch;

		while (rbt->spinlock == 1)
			;

		tdata->tx_stats[1][1][0]++;
		tdata->tx_starts++;
		if (__builtin_tbegin(0)) {
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

			/* Check the window versions. */
			if (window_versions[0] != GET_VERSION(gg) ||
			    window_versions[1] != GET_VERSION(g) ||
			    window_versions[2] != GET_VERSION(p) ||
			    window_versions[3] != GET_VERSION(q))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
			if (gg == &head && window_versions[4] != rbt->version)
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			if (IS_EXTERNAL_NODE(q)) {
				if (q->key == node[0]->key) {
					__builtin_tend(0);
					return inserted;
				}
	
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

				INC_VERSION(q);
				INC_VERSION(q->link[0]);
				INC_VERSION(q->link[1]);

				if (q == rbt->root) {
					q->is_red = 0;
					rbt->version++;
				} else {
					INC_VERSION(p);
				}
			} else if (IS_RED(q->link[0]) && IS_RED(q->link[1])) {
				/* Case 1: Color flip */
				tdata->insert_cases[0]++;

				q->is_red = 1;
				q->link[0]->is_red = 0;
				q->link[1]->is_red = 0;

				INC_VERSION(q);
				INC_VERSION(q->link[0]);
				INC_VERSION(q->link[1]);

				if (q == rbt->root) {
					q->is_red = 0;
					rbt->version++;
				} else {
					INC_VERSION(p);
				}
			}

			/* Fix red violation */
			if (IS_RED(q) && IS_RED(p)) {
				int dir2 = (gg->link[1] == g);

				g->is_red = 1;

				if (q == p->link[last]) {
					/* Case 2: Single rotation. */
					tdata->insert_cases[1]++;

					p->is_red = 0;
					gg->link[dir2] = rbt_rotate_single(g, !last);
	
					if (gg == &head) {
						rbt->root = gg->link[dir2];
						rbt->version++;
					}
	
					INC_VERSION(gg);
					INC_VERSION(g);
					INC_VERSION(p);
					INC_VERSION(q);

					/* Move on. */
					last = dir;
					dir = q->key < node[0]->key;
					g = p;
					p = q;
					q = p->link[dir];

					/* Get the version of the next window. */
					window_versions[0] = GET_VERSION(gg);
					window_versions[1] = GET_VERSION(g);
					window_versions[2] = GET_VERSION(p);
					window_versions[3] = GET_VERSION(q);
					window_versions[4] = rbt->version;

					__builtin_tend(0);
					continue;
				} else {
					/* Case 3: Double rotation. */
					tdata->insert_cases[2]++;

					q->is_red = 0;
					gg->link[dir2] = rbt_rotate_double(g, !last);
	
					if (gg == &head) {
						rbt->root = gg->link[dir2];
						rbt->version++;
					}
	
					INC_VERSION(gg);
					INC_VERSION(g);
					INC_VERSION(p);
					INC_VERSION(q);

					/* Move on. */
					last = q->key < node[0]->key;
					dir = q->link[last]->key < node[0]->key;
					g = q;
					p = g->link[last];
					q = p->link[dir];
	
					/* Get the version of the next window. */
					window_versions[0] = GET_VERSION(gg);
					window_versions[1] = GET_VERSION(g);
					window_versions[2] = GET_VERSION(p);
					window_versions[3] = GET_VERSION(q);
					window_versions[4] = rbt->version;

					__builtin_tend(0);
					continue;
				}
			}
	
			/* If we reach here means no rotation has been performed. */
			last = dir;
			dir = q->key < node[0]->key;
			
			if (gg == rbt->root) {
				if (IS_RED(rbt->root)) {
					rbt->root->is_red = 0;
					rbt->version++;
				}
			}
	
			/* Update helpers */
			if (g)
				gg = g;
			g = p;
			p = q;
			q = q->link[dir];

			/* Get the version of the next window. */
			window_versions[0] = GET_VERSION(gg);
			window_versions[1] = GET_VERSION(g);
			window_versions[2] = GET_VERSION(p);
			window_versions[3] = GET_VERSION(q);
			window_versions[4] = rbt->version;

			__builtin_tend(0);
			continue;
		} else {
			tdata->tx_aborts++;
			tdata->tx_stats[1][1][1]++;
			texasru_t texasru = __builtin_get_texasru();
			if (_TEXASRU_ABORT(texasru) && 
			    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
				tdata->tx_stats[1][1][5]++;
				tdata->tx_aborts_version_error++;
				goto try_from_scratch;
			} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
				tdata->tx_stats[1][1][4]++;
				tdata->tx_aborts_footprint_overflow++;
			} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
				tdata->tx_stats[1][1][2]++;
				tdata->tx_aborts_transaction_conflict++;
			} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
				tdata->tx_stats[1][1][3]++;
				tdata->tx_aborts_non_transaction_conflict++;
			} else {
				tdata->tx_stats[1][1][6]++;
				tdata->tx_aborts_rest++;
			}
			goto retry_window;
		}
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static inline int _rbt_delete_helper_fg_serial(rbt_t *rbt, int key,
                                               rbt_node_t *nodes_to_delete[2])
{
	if (!rbt->root)
		return 0;

	/* g = grandparent, p = parent, q = current */
	rbt_node_t *g, *p, *q;
	rbt_node_t *f = NULL;
	rbt_node_t head = { 0 };
	int dir = 1;
	int ret = 0;

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

		if (IS_EXTERNAL_NODE(q))
			break;

		if (IS_BLACK(q) && IS_BLACK(q->link[dir])) {
			if (IS_RED(q->link[!dir])) {
				INC_VERSION(p);
				INC_VERSION(q);
				INC_VERSION(q->link[!dir]);

				q->is_red = 1;
				q->link[!dir]->is_red = 0;
				p = p->link[last] = rbt_rotate_single(q, dir);
			} else if (IS_BLACK(q->link[!dir])) {
				rbt_node_t *s = p->link[!last];

				if (s) {
					if (IS_BLACK(s->link[!last]) && IS_BLACK(s->link[last])) {
						p->is_red = 0;
						q->is_red = 1;
						s->is_red = 1;

						INC_VERSION(g);
						INC_VERSION(p);
						INC_VERSION(q);
						INC_VERSION(s);
					} else {
						int dir2 = (g->link[1] == p);

						if (IS_RED(s->link[last])) {
							g->link[dir2] = rbt_rotate_double(p, last);
							p->is_red = 0;
							q->is_red = 1;

							INC_VERSION(g);
							INC_VERSION(p);
							INC_VERSION(q);
							INC_VERSION(s);
							INC_VERSION(g->link[dir2]);

						} else if (IS_RED(s->link[!last])) {
							g->link[dir2] = rbt_rotate_single(p, last);
							p->is_red = 0;
							q->is_red = 1;
							s->is_red = 1;
							s->link[!last]->is_red = 0;

							INC_VERSION(g);
							INC_VERSION(p);
							INC_VERSION(q);
							INC_VERSION(s);
						}
					}
				}
			}
		}
	}

	/* q is external. */
	if (q->key == key) {
		ret = 1;
		*nodes_to_delete = q;
		INC_VERSION(q);
		if (!p) {
			rbt->root = NULL;
			rbt->version++;
		} else {
			if (!g) {
				g = rbt->root;
				rbt->version++;
			}

			int last = g->key < key;
			dir = p->key < key;
			g->link[last] = p->link[!dir];
			INC_VERSION(g);
		}
	}

	/* Update root and make it BLACK. */
	if (rbt->root != head.link[1]) {
		rbt->root = head.link[1];
		rbt->version++;
	}
	if (rbt->root && rbt->root->is_red == 1) {
		rbt->root->is_red = 0;
		rbt->version++;
	}
	
	return ret;
}

static inline int _rbt_delete_helper_fg(rbt_t *rbt, int key, 
                                     rbt_node_t *nodes_to_delete[2],
                                     htm_fg_tdata_t *tdata)
{
	rbt_node_t *g, *p, *q; /* g = grandparent, p = parent, q = current */
	rbt_node_t *s = NULL; /* sibling */
	rbt_node_t head = { 0 };
	int dir = 1, last;
	int deleted = 0;
	unsigned long long window_versions[4]; /* g, p, q, rbt versions */
	int level;
	int retries = -1, window_retries = -1;

try_from_scratch:

	retries++;
	if (retries >= TX_NUM_RETRIES) {
		int ret = 0;
		tdata->tx_lacqs++;
		pthread_spin_lock(&rbt->spinlock);
		ret = _rbt_delete_helper_fg_serial(rbt, key, nodes_to_delete);
		pthread_spin_unlock(&rbt->spinlock);
		return ret;
	}

	level = 0;
	dir = 1;
	head.is_red = 0;
	head.link[0] = NULL;

	/* First transaction at the root. */
	while (1) {
		while (rbt->spinlock == 1)
			;

		tdata->tx_stats[2][0][0]++;
		tdata->tx_starts++;
		if (__builtin_tbegin(0)) {
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);

			/* Empty tree. */
			if (!rbt->root) {
				__builtin_tend(0);
				return 0;
			}

			/* One node in the tree. */
			if (IS_EXTERNAL_NODE(rbt->root)) {
				if (rbt->root->key == key) {
					nodes_to_delete[0] = rbt->root;
					nodes_to_delete[1] = NULL;
					INC_VERSION(rbt->root);

					rbt->root = NULL;
					deleted = 1;

					rbt->version++;
				}
				__builtin_tend(0);
				return deleted;
			}
		
			g = NULL;
			p = &head;
			q = p->link[1] = rbt->root;
	
			window_versions[0] = GET_VERSION(g);
			window_versions[1] = GET_VERSION(p);
			window_versions[2] = GET_VERSION(q);
			window_versions[3] = rbt->version;

			__builtin_tend(0);
			break;
		} else {
			tdata->tx_stats[2][0][1]++;
			tdata->tx_aborts++;
			texasru_t texasru = __builtin_get_texasru();
			if (_TEXASRU_ABORT(texasru) && 
			    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
				tdata->tx_stats[2][0][5]++;
				tdata->tx_aborts_version_error++;
			} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
				tdata->tx_stats[2][0][4]++;
				tdata->tx_aborts_footprint_overflow++;
			} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
				tdata->tx_stats[2][0][2]++;
				tdata->tx_aborts_transaction_conflict++;
			} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
				tdata->tx_stats[2][0][3]++;
				tdata->tx_aborts_non_transaction_conflict++;
			} else {
				tdata->tx_stats[2][0][6]++;
				tdata->tx_aborts_rest++;
			}

			goto try_from_scratch;
		}
	}

	while (1) {
		window_retries = -1;
retry_window:

		window_retries++;
		if (window_retries >= TX_NUM_RETRIES)
			goto try_from_scratch;

		while (rbt->spinlock == 1)
			;

		tdata->tx_stats[2][1][0]++;
		tdata->tx_starts++;
		if (__builtin_tbegin(0)) {
			if (rbt->spinlock == 1)
				__builtin_tabort(IMPLICIT_ABORT_GLOBAL_LOCK_TAKEN);
			/* Check the window_versions. */
			if (window_versions[0] != GET_VERSION(g) ||
			    window_versions[1] != GET_VERSION(p) ||
			    window_versions[2] != GET_VERSION(q))
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);
			if ((!g || g == &head) && window_versions[3] != rbt->version)
				__builtin_tabort(IMPLICIT_ABORT_VERSION_ERROR);

			last = dir;
			dir = q->key < key;
	
			if (IS_BLACK(q) && IS_BLACK(q->link[dir])) {
				if (IS_RED(q->link[!dir])) {
					/* Case 1. */
					INC_VERSION(p);
					INC_VERSION(q);
					INC_VERSION(q->link[!dir]);

					q->is_red = 1;
					q->link[!dir]->is_red = 0;
					p = p->link[last] = rbt_rotate_single(q, dir);
	
					if (q == rbt->root) {
						rbt->root = p;
						rbt->version++;
					}
				} else if (IS_BLACK(q->link[!dir])) {
					s = p->link[!last];
	
					if (s) {
						if (IS_BLACK(s->link[!last]) && IS_BLACK(s->link[last])) {
							p->is_red = 0;
							q->is_red = 1;
							s->is_red = 1;

							INC_VERSION(g);
							INC_VERSION(p);
							INC_VERSION(q);
							INC_VERSION(s);
						} else {
							int dir2 = (g->link[1] == p);
	
							if (IS_RED(s->link[last])) {
								g->link[dir2] = rbt_rotate_double(p, last);
								p->is_red = 0;
								q->is_red = 1;
	
								if (p == rbt->root) {
									rbt->root = head.link[1];
									rbt->root->is_red = 0;
									rbt->version++;
								}
	
								INC_VERSION(g);
								INC_VERSION(p);
								INC_VERSION(q);
								INC_VERSION(s);
								INC_VERSION(g->link[dir2]);
							} else if (IS_RED(s->link[!last])) {
								g->link[dir2] = rbt_rotate_single(p, last);
								p->is_red = 0;
								q->is_red = 1;
								s->is_red = 1;
								s->link[!last]->is_red = 0;
	
								if (p == rbt->root) {
									rbt->root = head.link[1];
									rbt->root->is_red = 0;
									rbt->version++;
								}
	
								INC_VERSION(g);
								INC_VERSION(p);
								INC_VERSION(q);
								INC_VERSION(s);
							}
						}
					}
				}
			}
	
			/* External child reached. */
			if (IS_EXTERNAL_NODE(q->link[dir])) {
				if (q->link[dir]->key == key) {
					INC_VERSION(p);
					INC_VERSION(q);
					INC_VERSION(q->link[dir]);
					INC_VERSION(q->link[!dir]);

					deleted = 1;
					nodes_to_delete[0] = q;
					nodes_to_delete[1] = q->link[dir];
			
					last = p->key < key;
					dir = q->key < key;
					p->link[last] = q->link[!dir];
			
					if (p == &head) {
						rbt->root = p->link[last];
						rbt->version++;
					}
				}
				__builtin_tend(0);
				return deleted;
			}
	
			/* Move to the next window. */
			g = p;
			p = q;
			q = q->link[dir];

			level++;

			window_versions[0] = GET_VERSION(g);
			window_versions[1] = GET_VERSION(p);
			window_versions[2] = GET_VERSION(q);
			window_versions[3] = rbt->version;
			
			__builtin_tend(0);
		} else {
			tdata->tx_stats[2][1][1]++;
			tdata->tx_aborts++;
			texasru_t texasru = __builtin_get_texasru();
			if (_TEXASRU_ABORT(texasru) && 
			    _TEXASRU_FAILURE_CODE(texasru) == IMPLICIT_ABORT_VERSION_ERROR) {
				tdata->tx_stats[2][1][5]++;
				tdata->tx_aborts_version_error++;
				goto try_from_scratch;
			} else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru)) {
				tdata->tx_stats[2][1][4]++;
				tdata->tx_aborts_footprint_overflow++;
			} else if (_TEXASRU_TRANSACTION_CONFLICT(texasru)) {
				tdata->tx_stats[2][1][2]++;
				tdata->tx_aborts_transaction_conflict++;
			} else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru)) {
				tdata->tx_stats[2][1][3]++;
				tdata->tx_aborts_non_transaction_conflict++;
			} else {
				tdata->tx_stats[2][1][6]++;
				tdata->tx_aborts_rest++;
			}
			goto retry_window;
		}
	}

	/* Unreachable. */
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

/* DEBUG */
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

	printf("%d[%s]\n", root->key, root->is_red ? "RED" : "BLA");

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

static int rbt_find_loop(rbt_node_t *root, int level)
{
	int ret;

	if (root) {
		if (root->link[0] == root || root->link[1] == root) {
			printf("Found loop at level %d (q = %d %d)\n", level, root->key, root->is_red);
			return 1;
		} else if (!IS_EXTERNAL_NODE(root) && (!root->link[0] || !root->link[1])) {
			printf("Found node with one child at level %d\n", level);
			return 2;
		}
	}

	if (!root) {
		return 0;
	}

	if (rbt_find_loop(root->link[0], level + 1))
		return 1;
	if (rbt_find_loop(root->link[1], level + 1))
		return 1;
	return 0;
}
static int _rbt_has_loop(rbt_t *rbt)
{
	return rbt_find_loop(rbt->root, 0);
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
	return htm_fg_tdata_new(tid);
}

void rbt_thread_data_print(void *thread_data)
{
	htm_fg_tdata_print(thread_data);
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
	htm_fg_tdata_add(d1, d2, dst);
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret;

	ret = _rbt_lookup_helper(rbt, thread_data, key, thread_data);

	return ret;
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret;
	rbt_node_t *nodes[2];

	nodes[0] = rbt_node_new(key, value);
	nodes[1] = rbt_node_new(key, value);

	ret = _rbt_insert_helper_fg(rbt, nodes, thread_data);

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

	ret = _rbt_delete_helper_fg(rbt, key, nodes_to_delete, thread_data);

//	if (ret) {
//		free(nodes_to_delete[0]);
//		free(nodes_to_delete[1]);
//	}

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
	return "links_td_external_fg_htm";
}

void rbt_print(void *rbt)
{
	rbt_print_struct(rbt);
}

int rbt_has_loop(void *rbt)
{
	return _rbt_has_loop(rbt);
}
