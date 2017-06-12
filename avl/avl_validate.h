#ifndef _AVL_VALIDATE_H_
#define _AVL_VALIDATE_H_

#include "avl_types.h"

static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes;
static int avl_violations, bst_violations;
static int _avl_validate_rec(avl_node_t *root, int _depth,
                             int range_min, int range_max)
{
	if (!root)
		return -1;

	avl_node_t *left = root->link[0];
	avl_node_t *right = root->link[1];

	total_nodes++;
	_depth++;

	/* BST violation? */
	if (range_min != -1 && root->key < range_min)
		bst_violations++;
	if (range_max != -1 && root->key > range_max)
		bst_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (!left || !right) {
		total_paths++;

		if (_depth <= min_path_len)
			min_path_len = _depth;
		if (_depth >= max_path_len)
			max_path_len = _depth;
	}

	/* Check subtrees. */
	int lheight = -1, rheight = -1;
	if (left)
		lheight = _avl_validate_rec(left, _depth, range_min, root->key);
	if (right)
		rheight = _avl_validate_rec(right, _depth, root->key + 1, range_max);

	/* AVL violation? */
	if (abs(lheight - rheight) > 1)
		avl_violations++;

	return MAX(lheight, rheight) + 1;
}

static inline int _avl_validate_helper(avl_node_t *root)
{
	int check_avl = 0, check_bst = 0;
	int check = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	avl_violations = 0;
	bst_violations = 0;

	_avl_validate_rec(root, 0, -1, -1);

	check_avl = (avl_violations == 0);
	check_bst = (bst_violations == 0);
	check = (check_avl && check_bst);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Valid AVL Tree: %s\n",
	       check ? "Yes [OK]" : "No [ERROR]");
	printf("  AVL Violation: %s\n",
	       check_avl ? "No [OK]" : "Yes [ERROR]");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Total nodes: %d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check;
}

#endif /* _AVL_VALIDATE_H_ */
