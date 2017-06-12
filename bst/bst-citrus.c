#include <stdio.h>
#include <stdlib.h>
#include "citrus.h"
#include "urcu.h"

static inline int _bst_warmup_helper(node root, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;

	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = insert(root, key, 0);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

//static int total_paths, total_nodes, bst_violations;
//static int min_path_len, max_path_len;
//static void _bst_validate_rec(volatile node_t *root, int _th)
//{
//	if (!root)
//		return;
//
//	volatile node_t *left = root->left;
//	volatile node_t *right = root->right;
//
//	if (root->key < INF0)
//		total_nodes++;
//	_th++;
//
//	/* BST violation? */
//	if (left && left->key >= root->key)
//		bst_violations++;
//	if (right && right->key < root->key)
//		bst_violations++;
//
//	/* We found a path (a node with at least one sentinel child). */
//	if (!left && !right && root->key < INF0) {
//		total_paths++;
//
//		if (_th <= min_path_len)
//			min_path_len = _th;
//		if (_th >= max_path_len)
//			max_path_len = _th;
//	}
//
//	/* Check subtrees. */
//	if (left)
//		_bst_validate_rec(left, _th);
//	if (right)
//		_bst_validate_rec(right, _th);
//}
//
//static inline int _bst_validate_helper(volatile node_t *root)
//{
//	int check_bst = 0;
//	total_paths = 0;
//	min_path_len = 99999999;
//	max_path_len = -1;
//	total_nodes = 0;
//	bst_violations = 0;
//
//	_bst_validate_rec(root, 0);
//
//	check_bst = (bst_violations == 0);
//
//	printf("Validation:\n");
//	printf("=======================\n");
//	printf("  BST Violation: %s\n",
//	       check_bst ? "No [OK]" : "Yes [ERROR]");
//	printf("  Tree size: %8d\n", total_nodes);
//	printf("  Total paths: %d\n", total_paths);
//	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
//	printf("\n");
//
//	return check_bst;
//}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(node));
	node root = init();
	initURCU(44);
	return (void *)root;
}

void *rbt_thread_data_new(int tid)
{
	urcu_register(tid);
//	return htm_fg_tdata_new(tid);
	return NULL;
}

void rbt_thread_data_print(void *thread_data)
{
//	htm_fg_tdata_print(thread_data);
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
//	htm_fg_tdata_add(d1, d2, dst);
}

int rbt_lookup(void *bst, void *thread_data, int key)
{
	int ret;
	ret = contains((node)bst, key);
	return (ret != -1);
}

int rbt_insert(void *bst, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = insert((node)bst, key, 0);
	return ret;
}

int rbt_delete(void *bst, void *thread_data, int key)
{
	int ret = 0;
	ret = delete((node)bst, key);
	return ret;
}

int rbt_validate(void *avl)
{
	int ret = 0;
	node root = avl;
	ret = citrus_validate_helper(root);
	return ret;
}

int rbt_warmup(void *bst, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _bst_warmup_helper((node)bst, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "bst_citrus";
}
