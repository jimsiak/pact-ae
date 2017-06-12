#ifndef _AVL_PRINT_H_
#define _AVL_PRINT_H_

#include <stdio.h>
#include "avl_types.h"

static void avl_print_rec(avl_node_t *root, int level)
{
	int i;

	if (root)
		avl_print_rec(root->link[1], level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%d[%d](%p)\n", root->key, root->height, root);

	avl_print_rec(root->link[0], level + 1);
}

static void avl_print_struct(avl_t *avl)
{
	if (avl->root == NULL)
		printf("[empty]");
	else
		avl_print_rec(avl->root, 0);
	printf("\n");
}

#endif /* _AVL_PRINT_H_ */
