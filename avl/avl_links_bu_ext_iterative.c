#include <assert.h>

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm.h"
#	if !defined(TX_NUM_RETRIES)
#		define TX_NUM_RETRIES 20
#	endif
#endif

#include "avl_types.h"
#include "avl_warmup.h"
#include "avl_utils.h"
#include "avl_validate.h"
#include "avl_links_bu_ext_thread_data.h"

//> Returns the level at which the lookup stopped.
static int _avl_lookup_helper(avl_t *avl, int key, int *found)
{
	int level = 0;
	avl_node_t *curr = avl->root;

	*found = 0;

	//> Empty tree.
	if (!curr)
		return level;

	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < key;
		curr = curr->link[dir];
		level++;
	}

	if (curr->key == key)
		*found = 1;

	return level;
}

/*
 * 'top' shows at the last occupied index of 'node_stack' which is the newly
 * added node to the tree.
 */
static inline void _avl_insert_fixup(avl_t *avl, int key,
                                     avl_node_t *node_stack[MAX_HEIGHT],
                                     int top, avl_thread_data_t *tdata)
{
	int lheight = -1, rheight = -1;
	avl_node_t *curr = NULL, *parent = NULL;
	int rotations = 0;

	top--; /* Ignore the newly added internal node. */
	while (top > 0) {
		curr = node_stack[top--];
		parent = node_stack[top];
		lheight = curr->link[0]->height;
		rheight = curr->link[1]->height;

		int balance = node_balance(curr);

		if (balance == 2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[0]);
			assert(balance2 != 0);

			if (balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 1);
				rotations += 1;
			} else if (balance2 == -1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 1);
				rotations += 2;
			} else {
				assert(0);
			}

			if (top == 0)
				avl->root = parent->link[dir_from_parent];

			break;
		} else if (balance == -2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[1]);
			assert(balance2 != 0);

			if (balance2 == -1) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 0);
				rotations += 1;
			} else if (balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 0);
				rotations += 2;
			} else {
				assert(0);
			}

			if (top == 0)
				avl->root = parent->link[dir_from_parent];

			break;
		}

		/* Update the height of curr. */
		int height_saved = curr->height;
		int height_new = MAX(lheight, rheight) + 1;
		curr->height = height_new;
		if (height_saved == height_new)
			break;
	}

#	ifdef VERBOSE_STATISTICS
	tdata->restructures_at_level[top-1] += rotations;
#	endif
}

static inline int _avl_insert_helper(avl_t *avl, avl_node_t *nodes[2],
                                     avl_thread_data_t *tdata)
{
	avl_node_t head = { 0 };
	avl_node_t *node_stack[MAX_HEIGHT];
	avl_node_t *curr = NULL;
	int top = -1;

	/* Empty tree */
	if (!avl->root) {
		avl->root = nodes[0];
		return 1;
	}

	head.height = 0;
	head.link[1] = avl->root;
	node_stack[++top] = &head;

	/* Traverse the tree until an external node is reached. */
	curr = avl->root;
	node_stack[++top] = curr;
	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < nodes[0]->key;
		curr = curr->link[dir];
		node_stack[++top] = curr;
	}

	/* Did we find the external node we were looking for? */
	if (curr->key == nodes[0]->key)
		return 0;

	/* Insert the new node and fixup any violations. */
	replace_external_node(curr, nodes);
	_avl_insert_fixup(avl, nodes[0]->key, node_stack, top, tdata);

	return 1;
}

/*
 * The bottom of the stack contains the first node that might be unbalanced.
 */
static inline void _avl_delete_fixup(avl_t *avl, int key,
                                     avl_node_t *node_stack[MAX_HEIGHT],
                                     int top, avl_thread_data_t *tdata)
{
	int lheight = -1, rheight = -1;
	avl_node_t *curr = NULL, *parent = NULL;
	int rotations = 0;

	while (top > 0) {
		curr = node_stack[top--];
		parent = node_stack[top];
		lheight = curr->link[0]->height;
		rheight = curr->link[1]->height;

		int balance = node_balance(curr);

		if (balance == 2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[0]);

			if (balance2 == 0 || balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 1);
				rotations += 1;
			} else if (balance2 == -1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 1);
				rotations += 2;
			} else {
				assert(0);
			}

			if (top == 0)
				avl->root = parent->link[dir_from_parent];

			continue;
		} else if (balance == -2) {
			int dir_from_parent = parent->key < key;
			int balance2 = node_balance(curr->link[1]);

			if (balance2 == -1 || balance2 == 0) {
				parent->link[dir_from_parent] = avl_rotate_single(curr, 0);
				rotations += 1;
			} else if (balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 0);
				rotations += 2;
			} else {
				assert(0);
			}

			if (top == 0)
				avl->root = parent->link[dir_from_parent];

			continue;
		}

		/* Update the height of curr. */
		int height_saved = curr->height;
		int height_new = MAX(lheight, rheight) + 1;
		curr->height = height_new;
		if (height_saved == height_new)
			break;
	}

#	ifdef VERBOSE_STATISTICS
	tdata->restructures_at_level[top-1] += rotations;
#	endif
}

static inline int _avl_delete_helper(avl_t *avl, int key,
                                     avl_node_t *nodes_to_delete[2],
                                     avl_thread_data_t *thread_data)
{
	avl_node_t head = { 0 };
	avl_node_t *node_stack[MAX_HEIGHT];
	avl_node_t *curr = NULL;
	int top = -1;

	/* Empty tree */
	if (!avl->root)
		return 0;

	head.link[1] = avl->root;
	node_stack[++top] = &head;

	/* Traverse the tree until an external node is reached. */
	curr = avl->root;
	node_stack[++top] = curr;
	while (!IS_EXTERNAL_NODE(curr)) {
		int dir = curr->key < key;
		curr = curr->link[dir];
		node_stack[++top] = curr;
	}

	/* Did we find the external node we were looking for? */
	if (curr->key != key)
		return 0;

	/* Unlink the leaf and fixup any violations. */
	avl_node_t *leaf = node_stack[top];
	if (top == 1) { /* Only one node in the tree. */
		nodes_to_delete[0] = leaf;
		nodes_to_delete[1] = NULL;
		avl->root = NULL;
	} else if (top == 2) { /* Leaf is root's child. */
		int dir = avl->root->key < key;
		nodes_to_delete[0] = avl->root;
		nodes_to_delete[1] = leaf;
		avl->root = avl->root->link[!dir];
	} else {
		avl_node_t *parent = node_stack[top-1];
		avl_node_t *gparent = node_stack[top-2];
		int dir_gparent = gparent->key < key;
		int dir_parent = parent->key < key;
		nodes_to_delete[0] = parent;
		nodes_to_delete[1] = leaf;
		gparent->link[dir_gparent] = parent->link[!dir_parent];
		top -= 2;
		_avl_delete_fixup(avl, key, node_stack, top, thread_data);
	}

	return 1;
}

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
	avl_thread_data_t *data = avl_thread_data_new(tid);

#	if defined(SYNC_CG_HTM)
	data->priv = tx_thread_data_new(tid);
#	endif

	return data;
}

void rbt_thread_data_print(void *thread_data)
{
	avl_thread_data_t *tdata = thread_data;
	avl_thread_data_print(tdata);

#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(tdata->priv);
#	endif

	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
	avl_thread_data_t *_d1 = d1, *_d2 = d2, *_dst = dst;

	avl_thread_data_add(_d1, _d2, _dst);

#	if defined(SYNC_CG_HTM)
	tx_thread_data_add(_d1->priv, _d2->priv, _dst->priv);
#	endif
}

int rbt_lookup(void *avl, void *thread_data, int key)
{
	avl_thread_data_t *tdata = thread_data;
	int ret;
	int found = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((avl_t *)avl)->global_lock);
#	elif defined(SYNC_CG_HTM)
	int naborts = tx_start(TX_NUM_RETRIES, tdata->priv, &((avl_t *)avl)->global_lock);
#	endif

	ret = _avl_lookup_helper(avl, key, &found);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((avl_t *)avl)->global_lock);
#	elif defined(SYNC_CG_HTM)
	int tx_ret = tx_end(tdata->priv, &((avl_t *)avl)->global_lock);
#	endif

	return found;
}

int rbt_insert(void *avl, void *thread_data, int key, void *value)
{
	avl_thread_data_t *tdata = thread_data;
	int ret;
	avl_node_t *nodes[2];

	nodes[0] = avl_node_new(key, value);
	nodes[1] = avl_node_new(key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((avl_t *)avl)->global_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((avl_t *)avl)->global_lock);
#	endif

	ret = _avl_insert_helper(avl, nodes, thread_data);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((avl_t *)avl)->global_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((avl_t *)avl)->global_lock);
#	endif

	if (!ret) {
		free(nodes[0]);
		free(nodes[1]);
	}

	return ret;
}

int rbt_delete(void *avl, void *thread_data, int key)
{
	avl_thread_data_t *tdata = thread_data;
	int ret;
	avl_node_t *nodes_to_delete[2] = {NULL, NULL};

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((avl_t *)avl)->global_lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->priv, &((avl_t *)avl)->global_lock);
#	endif

	ret = _avl_delete_helper(avl, key, nodes_to_delete, thread_data);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((avl_t *)avl)->global_lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->priv, &((avl_t *)avl)->global_lock);
#	endif

	if (ret) {
		free(nodes_to_delete[0]);
		free(nodes_to_delete[1]);
	}

	return ret;
}

int rbt_validate(void *avl)
{
	int ret;
	ret = _avl_validate_helper(((avl_t *)avl)->root);
	return ret;
}

int rbt_warmup(void *avl, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret;
	ret = _avl_warmup_helper((avl_t *)avl, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "avl_links_bu_external";
}
