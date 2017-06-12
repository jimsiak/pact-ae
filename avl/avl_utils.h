#ifndef _AVL_UTILS_H_
#define _AVL_UTILS_H_

#include "avl_types.h"

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )
#define MAX_HEIGHT 40 /* The max expected height of the paths in the tree. */
#define IS_EXTERNAL_NODE(node) \
    ( (node)->link[0] == NULL && (node)->link[1] == NULL )

static avl_node_t *avl_rotate_single(avl_node_t *root, int dir)
{
	avl_node_t *save = root->link[!dir];

	root->link[!dir] = save->link[dir];
	save->link[dir] = root;

	/* Update heights. */
	root->height = MAX(root->link[0]->height, root->link[1]->height) + 1;
	save->height = MAX(save->link[0]->height, save->link[1]->height) + 1;

#	ifdef USE_VERSIONING
	INC_VERSION(root);
	INC_VERSION(save);
#	endif

	return save;
}

static avl_node_t *avl_rotate_double(avl_node_t *root, int dir)
{
	root->link[!dir] = avl_rotate_single(root->link[!dir], !dir);
	return avl_rotate_single(root, dir);
}

static int node_balance(avl_node_t *n)
{
	if (!n)
		return 0;
	if (IS_EXTERNAL_NODE(n))
		return 0;

	return (n->link[0]->height - n->link[1]->height);
}

/**
 * Replace an external node with an internal with two children.
 * Example (* means red node);
 *
 *       8                4*
 *     /   \     =>     /    \
 *   NULL NULL         4      8
 *                   /   \  /   \
 *                 NULL  NULL  NULL
 **/
static inline void replace_external_node(avl_node_t *root,
                                         avl_node_t *nodes[2])
{
	root->link[0] = nodes[0];
	root->link[1] = nodes[1];
	root->height = 1;
	root->link[0]->height = 0;
	root->link[1]->height = 0;

	if (root->key > nodes[0]->key) {
		root->link[1]->key = root->key;
		root->key = root->link[0]->key;
	} else {
		root->link[0]->key = root->key;
	}
}

#endif /* _AVL_UTILS_H_ */
