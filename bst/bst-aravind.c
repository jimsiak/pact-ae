/*   
 *   File: bst-aravind.c
 *   Author: Tudor David <tudor.david@epfl.ch>
 *   Description: Aravind Natarajan and Neeraj Mittal. 
 *   Fast Concurrent Lock-free Binary Search Trees. PPoPP 2014
 *   bst-aravind.c is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 * 	     	      Tudor David <tudor.david@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * ASCYLIB is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */

#include <stdint.h>
#include <stdlib.h>
#include <limits.h>

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

#define CAS_PTR(a,b,c) __sync_val_compare_and_swap(a,b,c)

#define MAX_KEY INT_MAX
#define INF2 (MAX_KEY)
#define INF1 (MAX_KEY - 1)
#define INF0 (MAX_KEY - 2)

#define max(a,b) \
	({ __typeof__ (a) _a = (a); \
	   __typeof__ (b) _b = (b); \
	   _a > _b ? _a : _b; })

typedef int skey_t;
typedef void * sval_t;

typedef struct node_s {
	skey_t key;
	sval_t value;
	volatile struct node_s* right;
	volatile struct node_s* left;
//	char padding[CACHE_LINE_SIZE - sizeof(skey_t) - sizeof(sval_t) -
//	             2 * sizeof(volatile struct node_s *)];
//} __attribute__((packed)) __attribute__((aligned(CACHE_LINE_SIZE))) node_t;
//} __attribute__((packed)) node_t;
//} __attribute__((aligned(CACHE_LINE_SIZE))) node_t;
} node_t;

typedef struct seek_record_t {
	node_t *ancestor,
	       *successor,
	       *parent,
	       *leaf;
	char padding[CACHE_LINE_SIZE - 4 * sizeof(node_t *)];
} __attribute__((aligned(CACHE_LINE_SIZE))) seek_record_t;

//> Helper functions
static inline uint64_t GETFLAG(volatile node_t* ptr) {
	return ((uint64_t)ptr) & 1;
}
static inline uint64_t GETTAG(volatile node_t* ptr) {
	return ((uint64_t)ptr) & 2;
}
static inline uint64_t FLAG(node_t* ptr) {
	return (((uint64_t)ptr)) | 1;
}
static inline uint64_t TAG(node_t* ptr) {
	return (((uint64_t)ptr)) | 2;
}
static inline uint64_t UNTAG(node_t* ptr) {
	return (((uint64_t)ptr) & 0xfffffffffffffffd);
}
static inline uint64_t UNFLAG(node_t* ptr) {
	return (((uint64_t)ptr) & 0xfffffffffffffffe);
}
static inline node_t* ADDRESS(volatile node_t* ptr) {
	return (node_t*) (((uint64_t)ptr) & 0xfffffffffffffffc);
}

__thread seek_record_t* seek_record;

node_t *create_node(skey_t k, sval_t value) {
	node_t *new_node;

	XMALLOC(new_node, 1);

	new_node->left = NULL;
	new_node->right = NULL;
	new_node->key = k;
	new_node->value = value;
	asm volatile("" ::: "memory");

	return new_node;
}

node_t *initialize_tree() {
	node_t *r, *s, *inf0, *inf1, *inf2;

	r = create_node(INF2,0);
	s = create_node(INF1,0);
	inf0 = create_node(INF0,0);
	inf1 = create_node(INF1,0);
	inf2 = create_node(INF2,0);
    
    asm volatile("" ::: "memory");
    r->left = s;
    r->right = inf2;
    s->right = inf1;
    s->left= inf0;
    asm volatile("" ::: "memory");

    return r;
}

seek_record_t *bst_seek(skey_t key, node_t *node_r, int *nr_nodes_traversed) {
	*nr_nodes_traversed = 0;

	volatile seek_record_t seek_record_l;
	node_t *node_s = ADDRESS(node_r->left);
	seek_record_l.ancestor = node_r;
	seek_record_l.successor = node_s; 
	seek_record_l.parent = node_s;
	seek_record_l.leaf = ADDRESS(node_s->left);

	node_t* parent_field = (node_t*) seek_record_l.parent->left;
	node_t* current_field = (node_t*) seek_record_l.leaf->left;
	node_t* current = ADDRESS(current_field);

	while (current != NULL) {
		(*nr_nodes_traversed)++;

		if (!GETTAG(parent_field)) {
			seek_record_l.ancestor = seek_record_l.parent;
			seek_record_l.successor = seek_record_l.leaf;
		}
		seek_record_l.parent = seek_record_l.leaf;
		seek_record_l.leaf = current;

		parent_field = current_field;
		if (key < current->key)
			current_field = (node_t*) current->left;
		else
			current_field = (node_t*) current->right;

		current = ADDRESS(current_field);
	}
	seek_record->ancestor = seek_record_l.ancestor;
	seek_record->successor = seek_record_l.successor;
	seek_record->parent = seek_record_l.parent;
	seek_record->leaf = seek_record_l.leaf;
	return seek_record;
}

int bst_search(skey_t key, node_t *node_r) {
	int nr_nodes;
	bst_seek(key, node_r, &nr_nodes);
//	printf("nr_nodes %d\n", nr_nodes);
	return (seek_record->leaf->key == key);
}

int bst_cleanup(skey_t key) {
	node_t* ancestor = seek_record->ancestor;
	node_t* successor = seek_record->successor;
	node_t* parent = seek_record->parent;

	node_t** succ_addr;
	if (key < ancestor->key)
		succ_addr = (node_t**) &(ancestor->left);
	else
		succ_addr = (node_t**) &(ancestor->right);

	node_t** child_addr;
	node_t** sibling_addr;
	if (key < parent->key) {
		child_addr = (node_t**) &(parent->left);
		sibling_addr = (node_t**) &(parent->right);
	} else {
		child_addr = (node_t**) &(parent->right);
		sibling_addr = (node_t**) &(parent->left);
	}

	node_t* chld = *(child_addr);
	if (!GETFLAG(chld)) {
		chld = *(sibling_addr);
		asm volatile("");
		sibling_addr = child_addr;
	}

	while (1) {
		node_t* untagged = *sibling_addr;
		node_t* tagged = (node_t*)TAG(untagged);
		node_t* res = CAS_PTR(sibling_addr,untagged, tagged);
		if (res == untagged)
			break;
	}

	node_t* sibl = *sibling_addr;
	if ( CAS_PTR(succ_addr, ADDRESS(successor), UNTAG(sibl)) == ADDRESS(successor))
		return 1;
	return 0;
}

int bst_insert(skey_t key, sval_t val, node_t *node_r) {
	int nr_nodes;
	node_t *new_internal = NULL, *new_node = NULL;
	uint created = 0;

	while (1) {
		bst_seek(key, node_r, &nr_nodes);

		if (seek_record->leaf->key == key)
            return 0;

		node_t *parent = seek_record->parent;
		node_t *leaf = seek_record->leaf;

		node_t **child_addr;
		if (key < parent->key)
			child_addr = (node_t**) &(parent->left); 
		else
			child_addr = (node_t**) &(parent->right);

		if (created==0) {
			new_internal=create_node(max(key,leaf->key),0);
			new_node = create_node(key,val);
			created=1;
		} else {
			new_internal->key=max(key,leaf->key);
		}

		if ( key < leaf->key) {
			new_internal->left = new_node;
			new_internal->right = leaf; 
		} else {
			new_internal->right = new_node;
			new_internal->left = leaf;
		}

		node_t* result = CAS_PTR(child_addr, ADDRESS(leaf), ADDRESS(new_internal));
		if (result == ADDRESS(leaf))
			return 1;

		node_t* chld = *child_addr; 
		if ( (ADDRESS(chld)==leaf) && (GETFLAG(chld) || GETTAG(chld)) )
			bst_cleanup(key); 
	}
}

int bst_remove(skey_t key, node_t* node_r) {
	int nr_nodes;
	int injecting = 1; 
	node_t* leaf;
	sval_t val = 0;

	while (1) {
		bst_seek(key, node_r, &nr_nodes);
		val = seek_record->leaf->value;
		node_t *parent = seek_record->parent;

		node_t** child_addr;
		if (key < parent->key)
			child_addr = (node_t**) &(parent->left);
		else
			child_addr = (node_t**) &(parent->right);

		if (injecting == 1) {
			leaf = seek_record->leaf;
			if (leaf->key != key)
				return 0;

			node_t* lf = ADDRESS(leaf);
			node_t* result = CAS_PTR(child_addr, lf, FLAG(lf));
			if (result == ADDRESS(leaf)) {
				injecting = 0;
				int done = bst_cleanup(key);
				if (done)
					return 1;
			} else {
				node_t* chld = *child_addr;
				if ( (ADDRESS(chld) == leaf) && (GETFLAG(chld) || GETTAG(chld)) )
					bst_cleanup(key);
			}
		} else {
			if (seek_record->leaf != leaf) {
				return 1; 
			} else {
				int done = bst_cleanup(key);
				if (done)
					return 1;
			}
		}
    }
}

unsigned long long bst_size(volatile node_t* node) {
	if (node == NULL) return 0; 

	if ((node->left == NULL) && (node->right == NULL))
		if (node->key < INF0 )
			return 1;

	unsigned long long l = 0, r = 0;
	if ( !GETFLAG(node->left) && !GETTAG(node->left))
		l = bst_size(node->left);
	if ( !GETFLAG(node->right) && !GETTAG(node->right))
		r = bst_size(node->right);
	return l+r;
}

static inline int _bst_warmup_helper(node_t *root, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;

	XMALLOC(seek_record, 1);
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = bst_insert(key, NULL, root);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

static int total_paths, total_nodes, bst_violations;
static int min_path_len, max_path_len;
static void _bst_validate_rec(volatile node_t *root, int _th)
{
	if (!root)
		return;

	volatile node_t *left = root->left;
	volatile node_t *right = root->right;

	if (root->key < INF0) {
		total_nodes++;
		_th++;
	}

	/* BST violation? */
	if (left && left->key >= root->key)
		bst_violations++;
	if (right && right->key < root->key)
		bst_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (!left && !right && root->key < INF0) {
		total_paths++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_bst_validate_rec(left, _th);
	if (right)
		_bst_validate_rec(right, _th);
}

static inline int _bst_validate_helper(volatile node_t *root)
{
	int check_bst = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;

	_bst_validate_rec(root, 0);

	check_bst = (bst_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst;
}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(node_t));
	return (void *)initialize_tree();
}

void *rbt_thread_data_new(int tid)
{
	XMALLOC(seek_record, 1);
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
	ret = bst_search(key, bst);
	return ret;
}

int rbt_insert(void *avl, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = bst_insert(key, value, avl);
	return ret;
}

int rbt_delete(void *avl, void *thread_data, int key)
{
	int ret = 0;
	ret = bst_remove(key, avl);
	return ret;
}

int rbt_validate(void *avl)
{
	int ret = 1;
	volatile node_t *root = avl;
	ret = _bst_validate_helper(root);
	return ret;
}

int rbt_warmup(void *avl, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _bst_warmup_helper(avl, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "bst_aravind";
}
