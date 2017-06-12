#ifndef _AVL_WARMUP_H_
#define _AVL_WARMUP_H_

#include <stdlib.h> /* srand() etc. */
#include "avl_types.h"
#include "avl_utils.h"

/*
 * 'top' shows at the last occupied index of 'node_stack' which is the newly
 * added node to the tree.
 */
static inline void _avl_insert_fixup_warmup(avl_t *avl, int key,
                                            avl_node_t *node_stack[MAX_HEIGHT],
                                            int top)
{
	int lheight = -1, rheight = -1;
	avl_node_t *curr = NULL, *parent = NULL;

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
			} else if (balance2 == -1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 1);
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
			} else if (balance2 == 1) {
				parent->link[dir_from_parent] = avl_rotate_double(curr, 0);
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
}

static inline int _avl_insert_warmup(avl_t *avl, avl_node_t *nodes[2])
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
	_avl_insert_fixup_warmup(avl, nodes[0]->key, node_stack, top);

	return 1;
}

static inline int _avl_warmup_helper(avl_t *avl, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	avl_node_t *nodes[2];
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		nodes[0] = avl_node_new(key, NULL);
		nodes[1] = avl_node_new(key, NULL);

		ret = _avl_insert_warmup(avl, nodes);
		nodes_inserted += ret;

		if (!ret) {
			free(nodes[0]);
			free(nodes[1]);
		}
	}

	return nodes_inserted;
}

#endif /* _AVL_WARMUP_H_ */
